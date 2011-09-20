/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import static org.neo4j.backup.OnlineBackupExtension.parsePort;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.ComException;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.MasterTxIdGenerator.MasterTxIdGeneratorFactory;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveTxRollbackHook;
import org.neo4j.kernel.ha.TimeUtil;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class HighlyAvailableGraphDatabase extends AbstractGraphDatabase
        implements GraphDatabaseService, ResponseReceiver
{
    public static final String CONFIG_KEY_HA_MACHINE_ID = "ha.machine_id";
    public static final String CONFIG_KEY_HA_ZOO_KEEPER_SERVERS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_HA_SERVER = "ha.server";
    public static final String CONFIG_KEY_HA_CLUSTER_NAME = "ha.cluster_name";
    private static final String CONFIG_DEFAULT_HA_CLUSTER_NAME = "neo4j.ha";
    private static final int CONFIG_DEFAULT_PORT = 6361;
    public static final String CONFIG_KEY_HA_PULL_INTERVAL = "ha.pull_interval";
    public static final String CONFIG_KEY_ALLOW_INIT_CLUSTER = "ha.allow_init_cluster";

    private final String storeDir;
    private final Map<String, String> config;
    private final BrokerFactory brokerFactory;
    private final Broker broker;
    private volatile EmbeddedGraphDbImpl localGraph;
    private final int machineId;
    private volatile MasterServer masterServer;
    private ScheduledExecutorService updatePuller;
    private volatile long updateTime = 0;
    private volatile RuntimeException causeOfShutdown;
    private final long startupTime;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    private final StringLogger msgLog;

    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this( storeDir, config, null );
    }

    /**
     * Only for testing (and {@link org.neo4j.kernel.ha.BackupFromHaCluster})
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory )
    {
        if ( config == null )
        {
            throw new IllegalArgumentException( "null config, proper configuration required" );
        }
        initializeTxManagerKernelPanicEventHandler();
        this.startupTime = System.currentTimeMillis();
        this.storeDir = storeDir;
        this.config = config;
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        this.brokerFactory = brokerFactory != null ? brokerFactory : defaultBrokerFactory(
                this, config );
        this.machineId = getMachineIdFromConfig( config );
        this.broker = this.brokerFactory.create( this, config );
        this.msgLog = StringLogger.getLogger( storeDir );

        boolean allowInitFromConfig = getAllowInitFromConfig( config );
        startUp( allowInitFromConfig );
    }

    private void initializeTxManagerKernelPanicEventHandler()
    {
        kernelEventHandlers.add( new KernelEventHandler()
        {
            @Override public void beforeShutdown() {}

            @Override
            public void kernelPanic( ErrorState error )
            {
                if ( error == ErrorState.TX_MANAGER_NOT_OK )
                {
                    msgLog.logMessage( "TxManager not ok, doing internal restart" );
                    internalShutdown();
                    newMaster( null, new Exception( "Tx manager not ok" ) );
                }
            }

            @Override
            public Object getResource()
            {
                return null;
            }

            @Override
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
    }

    private void getFreshDatabaseFromMaster( Pair<Master, Machine> master )
    {
        master = master != null ? master : broker.getMasterReally();
        // Assume it's shut down at this point

        internalShutdown();
        moveAwayCurrentDatabase();

        Exception exception = null;
        for ( int i = 0; i < 10; i++ )
        {
            try
            {
                copyStoreFromMaster( master );
                return;
            }
            // TODO Maybe catch IOException and treat it more seriously?
            catch ( Exception e )
            {
                msgLog.logMessage( "Problems copying store from master", e );
                sleepWithoutInterruption( 1000, "" );
                exception = e;
            }
        }
        throw new RuntimeException( "Gave up trying to copy store from master", exception );
    }

    private void moveAwayCurrentDatabase()
    {
        // TODO This could be solved by moving the db directory to <dbPath>-broken-<date>
        // maybe <dbPath>/broken-<date> since we it may be the case that the current user
        // only has got permissions on the dbPath, not the parent.
        this.msgLog.logMessage( "Cleaning database " + storeDir + " to make way for new db from master" );

        File oldDir = new File( storeDir, "branched-" + System.currentTimeMillis() );
        oldDir.mkdirs();
        for ( File file : new File( storeDir ).listFiles() )
        {
            if (    // Exclude messages.log since it's good to have in one piece in
                    // the active directory.
                    !file.getName().equals( "messages.log" ) && 
                    
                    // Exclude any previous branched-??? directories, otherwise this
                    // becomes like a linked list of old directories.
                    (file.isDirectory() && file.getName().startsWith( "branched-" ) ) )
            {
                File dest = new File( oldDir, file.getName() );
                if ( !file.renameTo( dest ) )
                {
                    System.out.println( "Couldn't move " + file.getPath() );
                }
            }
        }
    }

    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDatabase.loadConfigurations( file );
    }

    private synchronized void startUp( boolean allowInit )
    {
        StoreId storeId = null;
        if ( !new File( storeDir, "neostore" ).exists() )
        {
            long endTime = System.currentTimeMillis()+10000;
            Exception exception = null;
            while ( System.currentTimeMillis() < endTime )
            {
                // Check if the cluster is up
                Pair<Master, Machine> master = broker.getMaster();
                master = master.first() != null ? master : broker.getMasterReally();
                if ( master != null && master.first() != null )
                { // Join the existing cluster
                    try
                    {
                        copyStoreFromMaster( master );
                        msgLog.logMessage( "copied store from master" );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        broker.getMasterReally();
                        msgLog.logMessage( "Problems copying store from master", e );
                    }
                }
                else if ( allowInit )
                { // Try to initialize the cluster and become master
                    exception = null;
                    StoreId myStoreId = new StoreId();
                    storeId = broker.createCluster( myStoreId );
                    if ( storeId.equals( myStoreId ) )
                    { // I am master
                        break;
                    }
                }
                // I am not master, and could not connect to the master:
                // wait for other machine(s) to join.
                sleepWithoutInterruption( 300, "Startup interrupted" );
            }

            if ( exception != null )
            {
                throw new RuntimeException( "Tried to join the cluster, but was unable to", exception );
            }
        }
        newMaster( null, storeId, new Exception() );
        localGraph();
    }

    private void sleepWithoutInterruption( long time, String errorMessage )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( errorMessage, e );
        }
    }

    private void copyStoreFromMaster( Pair<Master, Machine> master ) throws Exception
    {
        msgLog.logMessage( "Copying store from master" );
        Response<Void> response = master.first().copyStore( new SlaveContext( 0, machineId, 0, new Pair[0] ),
                new ToFileStoreWriter( storeDir ) );
        EmbeddedGraphDatabase copiedDb = new EmbeddedGraphDatabase( storeDir, stringMap( KEEP_LOGICAL_LOGS, "true" ) );
        try
        {
            MasterUtil.applyReceivedTransactions( response, copiedDb, MasterUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
        }
        msgLog.logMessage( "Done copying store from master" );
    }

    private EmbeddedGraphDbImpl localGraph()
    {
        if ( localGraph == null )
        {
            if ( causeOfShutdown != null )
            {
                throw causeOfShutdown;
            }
            else
            {
                throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                		"maybe not started yet or in the middle of master/slave swap?" );
            }
        }
        return localGraph;
    }

    private BrokerFactory defaultBrokerFactory( final GraphDatabaseService graphDb,
            final Map<String, String> config )
    {
        return new BrokerFactory()
        {
            @Override
            public Broker create( GraphDatabaseService graphDb, Map<String, String> config )
            {
                return new ZooKeeperBroker( graphDb,
                        getClusterNameFromConfig( config ),
                        getMachineIdFromConfig( config ),
                        getZooKeeperServersFromConfig( config ),
                        getHaServerFromConfig( config ),
                        getBackupPortFromConfig( config ),
                        HighlyAvailableGraphDatabase.this );
            }
        };
    }

    /**
     * @return the port for the backup server if that is enabled, or 0 if disabled.
     */
    private static int getBackupPortFromConfig( Map<?, ?> config )
    {
        String backupConfig = (String) config.get( ENABLE_ONLINE_BACKUP );
        Integer port = parsePort( backupConfig );
        return port != null ? port : 0;
    }

    private static String getClusterNameFromConfig( Map<?, ?> config )
    {
        String clusterName = (String) config.get( CONFIG_KEY_HA_CLUSTER_NAME );
        if ( clusterName == null ) clusterName = CONFIG_DEFAULT_HA_CLUSTER_NAME;
        return clusterName;
    }

    private static String getHaServerFromConfig( Map<?, ?> config )
    {
        String haServer = (String) config.get( CONFIG_KEY_HA_SERVER );
        if ( haServer == null )
        {
            InetAddress host = null;
            try
            {
                host = InetAddress.getLocalHost();
            }
            catch ( UnknownHostException hostBecomesNull )
            {
                // handled by null check
            }
            if ( host == null )
            {
                throw new IllegalStateException(
                        "Could not auto configure host name, please supply " + CONFIG_KEY_HA_SERVER );
            }
            haServer = host.getHostAddress() + ":" + CONFIG_DEFAULT_PORT;
        }
        return haServer;
    }

    private static boolean getAllowInitFromConfig( Map<?, ?> config )
    {
        String allowInit = (String) config.get( CONFIG_KEY_ALLOW_INIT_CLUSTER );
        if ( allowInit == null ) return true;
        return Boolean.parseBoolean( allowInit );
    }

    private static String getZooKeeperServersFromConfig( Map<String, String> config )
    {
        return config.get( CONFIG_KEY_HA_ZOO_KEEPER_SERVERS );
    }

    private static int getMachineIdFromConfig( Map<String, String> config )
    {
        // Fail fast if null
        return Integer.parseInt( config.get( CONFIG_KEY_HA_MACHINE_ID ) );
    }

    public Broker getBroker()
    {
        return this.broker;
    }

    public void pullUpdates()
    {
        try
        {
            if ( masterServer == null )
            {
                receive( broker.getMaster().first().pullUpdates( getSlaveContext( -1 ) ) );
            }
        }
        catch ( ZooKeeperException e )
        {
            newMaster( null, e );
            throw e;
        }
        catch ( ComException e )
        {
            newMaster( null, e );
            throw e;
        }
    }

    private void updateTime()
    {
        this.updateTime = System.currentTimeMillis();
    }

    long lastUpdateTime()
    {
        return this.updateTime;
    }

    @Override
    public Config getConfig()
    {
        return localGraph().getConfig();
    }

    @Override
    public String getStoreDir()
    {
        return this.storeDir;
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localGraph().getManagementBeans( type );
    }

    protected synchronized void reevaluateMyself( Pair<Master, Machine> master, StoreId storeId )
    {
        if ( master == null )
        {
            master = broker.getMasterReally();
        }
        boolean restarted = false;
        boolean iAmCurrentlyMaster = masterServer != null;
        msgLog.logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                "] (I am master=" + iAmCurrentlyMaster + ", " + localGraph + ")" );
        if ( master.other().getMachineId() == machineId )
        {   // I am the new master
            if ( this.localGraph == null || !iAmCurrentlyMaster )
            {   // I am currently a slave, so restart as master
                internalShutdown();
                startAsMaster( storeId );
                restarted = true;
            }
            // fire rebound event
            broker.rebindMaster();
        }
        else
        {   // Someone else is the new master
            broker.notifyMasterChange( master.other() );
            if ( this.localGraph == null || iAmCurrentlyMaster )
            {   // I am currently master, so restart as slave.
                // This will result in clearing of free ids from .id files, see SlaveIdGenerator.
                internalShutdown();
                startAsSlave( storeId );
                restarted = true;
            }
            else
            {   // I am already a slave, so just forget the ids I got from the previous master
                ((SlaveIdGeneratorFactory) getConfig().getIdGeneratorFactory()).forgetIdAllocationsFromMaster();
            }

            tryToEnsureIAmNotABrokenMachine( broker.getMaster() );
        }
        if ( restarted )
        {
            doAfterLocalGraphStarted();
        }
    }

    private void doAfterLocalGraphStarted()
    {
        broker.setConnectionInformation( this.localGraph.getKernelData() );
        for ( TransactionEventHandler<?> handler : transactionEventHandlers )
        {
            localGraph().registerTransactionEventHandler( handler );
        }
        for ( KernelEventHandler handler : kernelEventHandlers )
        {
            localGraph().registerKernelEventHandler( handler );
        }
    }

    private void startAsSlave( StoreId storeId )
    {
        msgLog.logMessage( "Starting[" + machineId + "] as slave", true );
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, storeId, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxRollbackHook( broker, this ),
                new ZooKeeperLastCommittedTxIdSetter( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        instantiateAutoUpdatePullerIfConfigSaysSo();
        msgLog.logMessage( "Started as slave", true );
    }

    private void startAsMaster( StoreId storeId )
    {
        msgLog.logMessage( "Starting[" + machineId + "] as master", true );
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, storeId, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                new MasterTxIdGeneratorFactory( broker ),
                CommonFactories.defaultTxFinishHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
        msgLog.logMessage( "Started as master", true );
    }

    private void tryToEnsureIAmNotABrokenMachine( Pair<Master, Machine> master )
    {
        if ( master.other().getMachineId() == machineId )
        {
            return;
        }
        else if ( master.first() == null )
        {
            // Temporarily disconnected from ZK
            RuntimeException cause = new RuntimeException( "Unable to get master from ZK" );
            shutdown( cause, false );
            throw cause;
        }

        XaDataSource nioneoDataSource = getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        long highestCommonTxId = Math.min( myLastCommittedTx, master.other().getLastCommittedTxId() );
        int masterForMyHighestCommonTxId = -1;
        try
        {
            masterForMyHighestCommonTxId = nioneoDataSource.getMasterForCommittedTx( highestCommonTxId );
        }
        catch ( IOException e )
        {
            // This is quite dangerous to just catch here... but the special case is
            // where this db was just now copied from the master where there's data,
            // but no logical logs were transfered and hence no masterId info is here
            msgLog.logMessage( "Couldn't get master ID for txId " + highestCommonTxId +
                    ". It may be that a log file is missing due to the db being copied from master?", e );
            return;
        }

        int masterForMastersHighestCommonTxId = master.first().getMasterIdForCommittedTx( highestCommonTxId ).response();

        // Compare those two, if equal -> good
        if ( masterForMyHighestCommonTxId == masterForMastersHighestCommonTxId )
        {
            msgLog.logMessage( "Master id for last committed tx ok with highestCommonTxId=" +
                    highestCommonTxId + " with masterId=" + masterForMyHighestCommonTxId, true );
            return;
        }
        else
        {
            String msg = "Branched data, I (machineId:" + machineId + ") think machineId for txId (" +
                    highestCommonTxId + ") is " + masterForMyHighestCommonTxId + ", but master (machineId:" +
                    master.other().getMachineId() + ") says that it's " + masterForMastersHighestCommonTxId;
            msgLog.logMessage( msg, true );
            RuntimeException exception = new BranchedDataException( msg );
            shutdown( exception, false );
            throw exception;
        }
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        String pullInterval = this.config.get( CONFIG_KEY_HA_PULL_INTERVAL );
        if ( pullInterval != null )
        {
            long timeMillis = TimeUtil.parseTimeMillis( pullInterval );
            updatePuller = new ScheduledThreadPoolExecutor( 1 );
            updatePuller.scheduleWithFixedDelay( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        pullUpdates();
                    }
                    catch ( Exception e )
                    {
                        msgLog.logMessage( "Pull updates failed", e  );
                    }
                }
            }, timeMillis, timeMillis, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public Transaction beginTx()
    {
        return localGraph().beginTx();
    }

    @Override
    public Node createNode()
    {
        return localGraph().createNode();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return localGraph().getAllNodes();
    }

    @Override
    public Node getNodeById( long id )
    {
        return localGraph().getNodeById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return localGraph().getReferenceNode();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return localGraph().getRelationshipById( id );
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return localGraph().getRelationshipTypes();
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        this.kernelEventHandlers.add( handler );
        return localGraph().registerKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return localGraph().registerTransactionEventHandler( handler );
    }

    public synchronized void internalShutdown()
    {
        msgLog.logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this + ", masterServer=" + masterServer, true );
        if ( this.updatePuller != null )
        {
            msgLog.logMessage( "Internal shutdown updatePuller", true );
            this.updatePuller.shutdown();
            msgLog.logMessage( "Internal shutdown updatePuller DONE", true );
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            msgLog.logMessage( "Internal shutdown masterServer", true );
            this.masterServer.shutdown();
            msgLog.logMessage( "Internal shutdown masterServer DONE", true );
            this.masterServer = null;
        }
        if ( this.localGraph != null )
        {
            msgLog.logMessage( "Internal shutdown localGraph", true );
            this.localGraph.shutdown();
            msgLog.logMessage( "Internal shutdown localGraph DONE", true );
            this.localGraph = null;
        }
        msgLog.flush();
        StringLogger.close( storeDir );
    }

    private synchronized void shutdown( RuntimeException cause, boolean shutdownBroker )
    {
//        if ( causeOfShutdown != null )
//        {
//            return;
//        }

        causeOfShutdown = cause;
        msgLog.logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( shutdownBroker && this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown();
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown( new IllegalStateException(), true );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return localGraph().unregisterKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return localGraph().unregisterTransactionEventHandler( handler );
    }

    @Override
    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        XaDataSourceManager localDataSourceManager =
            getConfig().getTxModule().getXaDataSourceManager();
        Collection<XaDataSource> dataSources = localDataSourceManager.getAllRegisteredDataSources();
        @SuppressWarnings("unchecked")
        Pair<String, Long>[] txs = new Pair[dataSources.size()];
        int i = 0;
        for ( XaDataSource dataSource : dataSources )
        {
            txs[i++] = Pair.of( dataSource.getName(), dataSource.getLastCommittedTxId() );
        }
        return new SlaveContext( startupTime, machineId, eventIdentifier, txs );
    }

    @Override
    public <T> T receive( Response<T> response )
    {
        try
        {
            MasterUtil.applyReceivedTransactions( response, this, MasterUtil.NO_ACTION );
            updateTime();
            return response.response();
        }
        catch ( IOException e )
        {
            newMaster( broker.getMaster(), e );
            throw new RuntimeException( e );
        }
    }

    @Override
    public void newMaster( Pair<Master, Machine> master, Exception e )
    {
        newMaster( master, null, e );
    }

    private synchronized void newMaster( Pair<Master, Machine> master, StoreId storeId, Exception e )
    {
        try
        {
            doNewMaster( master, storeId, e );
        }
        catch ( BranchedDataException bde )
        {
            System.out.println( "Branched data occured, retrying" );
            getFreshDatabaseFromMaster( master );
            doNewMaster( master, storeId, bde );
        }
    }

    private void doNewMaster( Pair<Master, Machine> master, StoreId storeId, Exception e )
    {
        try
        {
            msgLog.logMessage( "newMaster(" + master + ") called", e, true );
            reevaluateMyself( master, storeId );
        }
        catch ( ZooKeeperException ee )
        {
            msgLog.logMessage( "ZooKeeper exception in newMaster", ee );
        }
        catch ( ComException ee )
        {
            msgLog.logMessage( "Communication exception in newMaster", ee );
        }
        // BranchedDataException will escape from this method since the catch clause below
        // sees to that.
        catch ( Throwable t )
        {
            t.printStackTrace();
            msgLog.logMessage( "Reevaluation ended in unknown exception " + t
                    + " so shutting down", true );
            shutdown( t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException( t ), false );
            if ( t instanceof RuntimeException )
            {
                throw (RuntimeException) t;
            }
            throw new RuntimeException( t );
        }
    }

    public MasterServer getMasterServerIfMaster()
    {
        return masterServer;
    }

    int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return broker.iAmMaster();
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public IndexManager index()
    {
        return localGraph().index();
    }

    // Only for testing purposes, simulates a network outage almost
    public void shutdownBroker()
    {
        this.broker.shutdown();
    }
}
