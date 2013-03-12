'''
1. Initialize:
    Gets a list of sentinel processes and retrieves master and slave instances from a chosen sentinel.
    Begin listening to events published by chosen sentinel.
    (COMPLETE)
2. On failed write to master redis instace
    Initialize driver failover state *a
    (COMPLETE)

3. On sentinel events
    (IMPLEMENT THESE)
    +odown (master instance)
    +sdown (master instance)
        Init driver failover state *a
    -odown (master instance)
    -sdown (master instance)
        Exit driver failover state *b

    +sentinel
        add sentinel info to our list of sentinel clients
    -sentinel
        remove sentinel from our list of sentinel clients
    +switchmaster (new master instance)
        Reset masters and slaves and exit the failover state if necessary.
    +slave (master instance)
        track the slave of the given master instance


4. On closed connection to sentinel
    renegotiate the sentinel process to be used for

*a:
    Create pipeline and begin batching all write requests to pipeline(COMPLETE).  Reroute all reads to an arbitrary slave instance.
*b:
    execute pipeline commands on new master redis instance.  Reads return to master instance(COMPLETE).

TODO: go through code and handle keyerrors (python doesnt return null for attempting to access unset key)
        use .get(key) to test existence of key in dict
        use len(list) to test existence of index in list
TODO: Negotiator may block so may have to be a thread
'''

import re
import redis
import threading
from time import sleep

class Negotiator(object):
    defaultHost = 'localhost'
    pipelines = {}
    clients = {}
    _listener = None
    _currentSentinel = None
    _sentinels = []
    _masters = {}
    _slaves = {}

    def __init__(self, sentinelPorts):
        self._sentinels = [ redis.StrictRedis(self.defaultHost, port) for port in sentinelPorts ]
        self.negotiateCurrentSentinel()
        self._setMasters()
        for masterName in self._masters:
            self._setSlaves(masterName)

    def negotiateCurrentSentinel(self):
        if not len(self._sentinels):
            raise 'No functioning sentinels'
        if not self._currentSentinel:
            self._currentSentinel = self._sentinels[0]
        if self._currentSentinel.ping():
            pass
        else:
            newSentinelIndex = currentSentinelIndex = self._sentinels.index(self._currentSentinel)
            while True:
                newSentinelIndex = (newSentinelIndex + 1) % len(self._sentinels)
                if newSentinelIndex == currentSentinelIndex:
                    return False
                elif self._sentinels[newSentinelIndex].ping():
                    self._currentSentinel = self._sentinels[newSentinelIndex]
                    break
        self._subscribeToCurrentSentinel()

    # Set the slaves for the master in this redis db pool
    def _setSlaves(self, masterName):
        slaves = self._currentSentinel.execute_command('sentinel', 'slaves', masterName)
        ''' Change two dimensional array to an array of dicts '''
        self._slaves[masterName] = [{slave[i]: slave[i+1] for i in range(0, len(slave), 2)} for slave in slaves]
        return self._slaves[masterName]

    # Set the masters for the monitored redis instances
    def _setMasters(self):
        masters = self._currentSentinel.execute_command('sentinel', 'masters')
        ''' Change two dimensional array to a two-dimensional dict '''
        self._masters = { masterDict['name']: masterDict for masterDict in [{master[i]: master[i+1] for i in range(0, len(master), 2)} for master in masters] }
        return self._masters

    def getClient(self, masterName, **kwargs):
        if self.clients.get(masterName):
            return self.clients[masterName]
        negotiatorClient = self.NegotiatorClient(masterName, self, host=self._masters[masterName]['ip'], port=int(self._masters[masterName]['port']), **kwargs)
        self.clients[masterName] = negotiatorClient
        return self.clients[masterName]

    # Set listener to sentinel pubsub.  Do nothing if listener already set.
    def _subscribeToCurrentSentinel(self):
        if self._listener and self._listener.sentinel == self._currentSentinel:
            return
        self._subscription = self._currentSentinel.pubsub()
        self._listener = self.Listener(self)
        self._listener.start()

    # Begin methods to handle publish stream from Sentinel
    _SUBSCRIPTION_HANDLES = {
        '+sdown':'_subDown',
        '-sdown':'_subUp',
        '+odown':'_objDown',
        '-odown':'_objUp',
        '+sentinel':'_newSentinel',
        '+slave':'_newSlave',
        '+switch-master':'_switchMaster',
        '+reboot':'_rebootInstance'
    }

    def _switchMaster(self, instanceInfo):
        # TODO: if the master changes, reset it.  May be obselete due to the name staying the same
        #self._enterFailsafeState(instanceInfo['name'])
        pass

    def _objDown(self, instanceInfo):
        if instanceInfo['type'] == 'master':
            self._enterFailsafeState(instanceInfo['name'])
        elif instanceInfo['type'] == 'sentinel':
            pass
        elif instanceInfo['type'] == 'slave':
            pass

    def _objUp(self, instanceInfo):
        if instanceInfo['type'] == 'master':
            self._exitFailsafeState(instanceInfo['name'])
        elif instanceInfo['type'] == 'sentinel':
            pass
        elif instanceInfo['type'] == 'slave':
            pass

    def _subDown(self, instanceInfo):
        if instanceInfo['type'] == 'master':
            self._enterFailsafeState(instanceInfo['name'])
        elif instanceInfo['type'] == 'sentinel':
            pass
        elif instanceInfo['type'] == 'slave':
            pass

    def _subUp(self, instanceInfo):
        if instanceInfo['type'] == 'master':
            self._exitFailsafeState(instanceInfo['name'])
        else:
            print(instanceInfo['type'] + ' just came up')

    def _newSlave(self, instanceInfo):
        print(instanceInfo['type'] + ' just came up')
        self._setSlaves(instanceInfo['masterName']

    def _newSentinel(self, instanceInfo):
        print(instanceInfo['type'] + ' just came up')
        self.addSentinel(instanceInfo['host'], instanceInfo['port'])
        pass

    def _rebootInstance(self, instanceInfo):
        print(instanceInfo['type'] + ' just rebooted')
        pass

    def _enterFailsafeState(self, masterName):
        print('NEGOTIATOR::MASTERDOWN => attempting to enter failsafe')
        masterDict = self._masters[masterName]
        if getattr(masterDict, 'failsafeState', False):
            return # exit silently if already in failsafe state
        if not self.clients.get(masterName): # may not need this.  Should always be a client for each master
            return # TODO: maybe raise an error if no client exists with the mastername
        client = self.clients[masterName]
        pipeline = None
        ''' gets connection to arbitrary slave.  need to make sure slave works for reads. '''
        if not len(self.slaves[masterName]):
            pipeline = client.pipeline()
            masterDict['failsafeState'] = 'rw'
        else:
            slaveConnectionPool = self._getConnectionPoolFor(self._slaves[masterName][0])
            client.connection_pool = slaveConnectionPool
            masterDict['failsafeState'] = 'w'

        self.pipelines[masterName] = pipeline

    def _exitFailsafeState(self, masterName):
        print('NEGOTIATOR::MASTERUP => attempting to exit failsafe state')
        if not self.clients.get(masterName):
            return
        client = self.clients[masterName]
        masterConnectionPool = self._getConnectionPoolFor(self._masters[masterName])
        client.connection_pool = masterConnectionPool

        ''' Think of a case where it is not desireable to assume pipeline exists '''
        if not self.pipelines.get(masterName):
            return
        self.pipelines[masterName].connection_pool = masterConnectionPool
        self.pipelines[masterName].execute()
        ''' may only want to delete pipeline after execute succeeds '''
        del self.pipelines[masterName]

    ''' Parse message into useable data structure and send to appropriate handler '''
    def _handleMessage(self, message):
        if not (message and type(message['data']) == str):
            return
        instanceInfo = self._getInstanceInfo(message['data'])
        print(instanceInfo)
        handle = self._SUBSCRIPTION_HANDLES.get(message['channel'])
        print('handled by: ' + str(handle))
        if not handle:
            return
        try:
            self.__getattribute__(handle)(instanceInfo)
        except AttributeError:
            print('handler for ' + message['channel'] + ' not implemented')

    def _getInstanceInfo(self, infoStr):
        infoArray = infoStr.split()
        print(infoArray)
        infoDict = {
            'type': infoArray[0],
            'name': infoArray[1],
            'host': infoArray[2],
            'port': infoArray[3],
        }
        if infoDict['type'] == 'slave':
            infoDict['masterName'] = infoArray[5]

        return infoDict

    def _getConnectionPoolFor(self, redisInstanceDict):
        redisClient = redis.StrictRedis(host=redisInstanceDict['ip'], port=int(redisInstanceDict['port']))
        return redisClient.connection_pool

    class Listener(threading.Thread):
        daemon = True
        def __init__(self, negotiator):
            threading.Thread.__init__(self)
            self.negotiator = negotiator
            self.subscription = self.negotiator._subscription
            self.sentinel = negotiator._currentSentinel

        def run(self):
            self.subscription.psubscribe('*')
            while self.subscription and self.subscription == self.negotiator._subscription:
                try:
                    messages = self.subscription.listen()
                    for message in messages:
                        self.negotiator._handleMessage(message)
                except redis.exceptions.ConnectionError:
                    # TODO: if sentinel comes offline, we should handle it
                    print('sentinel connection closed')
                    break
                sleep(1)

    # used to run negotiator middleware for noral redis requests
    class NegotiatorClient(redis.StrictRedis):
        # TODO: populate write commands
        _writeCommands = {
            'push': 1
        }

        def __init__(self, masterName, negotiator, **kwargs):
            self.name = masterName
            self.negotiator = negotiator
            mHost = kwargs.get('host')
            mPort = kwargs.get('port')
            redis.StrictRedis.__init__(self, host=mHost, port=mPort)

        def execute_command(self, *args, **options):
            pipeline = self._getPipeline()
            if pipeline and self._isWriteCommand(args[0]):
                return pipeline.execute_command(*args, **options)
            try:
                return redis.StrictRedis.execute_command(self, *args, **options)
            except redis.exceptions.ConnectionError:
                print('entering failsafe state')
                self.negotiator._enterFailsafeState(self.name)
                self.execute_command(*args, **options)

        def _getPipeline(self):
            return self.negotiator.pipelines.get(self.name)

        def _isWriteCommand(self, command):
            commandLow = command.lower()
            if re.search( '(pop)|(set)|(del)', commandLow ):
                return True
            else:
                return bool( self._writeCommands.get(commandLow) )

