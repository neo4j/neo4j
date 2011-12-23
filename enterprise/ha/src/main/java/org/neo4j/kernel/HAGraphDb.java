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

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
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
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.MasterTxHook;
import org.neo4j.kernel.ha.MasterTxIdGenerator.MasterTxIdGeneratorFactory;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxHook;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class HAGraphDb extends AbstractGraphDatabase
        implements GraphDatabaseService, ResponseReceiver
{
    private final Map<String, String> config;
    private final BrokerFactory brokerFactory;
    private volatile Broker broker;
    private volatile EmbeddedGraphDbImpl localGraph;
    private final int machineId;
    private volatile MasterServer masterServer;
    private ScheduledExecutorService updatePuller;
    private volatile long updateTime = 0;
    private volatile Throwable causeOfShutdown;
    private final long startupTime;
    private final BranchedDataPolicy branchedDataPolicy;
    private final HaConfig.SlaveUpdateMode slaveUpdateMode;
    private final int readTimeout;
    /*
     *  True iff it is ok to pull updates. Used to control the
     *  update puller during master switches, to reduce could not connect
     *  log statements. More elegant that stopping and starting the executor.
     */
    private volatile boolean pullUpdates;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HAGraphDb( String storeDir, Map<String, String> config )
    {
        this( storeDir, config, null );
    }

    /**
     * Only for testing
     */
    public HAGraphDb( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory )
    {
        super( storeDir );
        if ( config == null )
        {
            throw new IllegalArgumentException( "null config, proper configuration required" );
        }
        this.startupTime = System.currentTimeMillis();
        this.config = config;
        initializeTxManagerKernelPanicEventHandler();
        this.readTimeout = HaConfig.getClientReadTimeoutFromConfig( config );
        this.slaveUpdateMode = HaConfig.getSlaveUpdateModeFromConfig( config );
        this.machineId = HaConfig.getMachineIdFromConfig( config );
        this.branchedDataPolicy = HaConfig.getBranchedDataPolicyFromConfig( config );
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        this.brokerFactory = brokerFactory != null ? brokerFactory : defaultBrokerFactory();
        this.broker = this.brokerFactory.create( this, config );
        this.pullUpdates = false;
        startUp( HaConfig.getAllowInitFromConfig( config ) );
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
                    getMessageLog().logMessage( "TxManager not ok, doing internal restart" );
                    internalShutdown( true );
                    newMaster( new Exception( "Tx manager not ok" ) );
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

    private void getFreshDatabaseFromMaster( )
    {
        Pair<Master, Machine> master = broker.getMasterReally( true );
        // Assume it's shut down at this point

        internalShutdown( false );
        makeWayForNewDb();

        Exception exception = null;
        for ( int i = 0; i < 60; i++ )
        {
            try
            {
                copyStoreFromMaster( master );
                return;
            }
            // TODO Maybe catch IOException and treat it more seriously?
            catch ( Exception e )
            {
                getMessageLog().logMessage( "Problems copying store from master", e );
                sleepWithoutInterruption( 1000, "" );
                exception = e;
                master = broker.getMasterReally( true );
            }
        }
        throw new RuntimeException( "Gave up trying to copy store from master", exception );
    }

    void makeWayForNewDb()
    {
        this.getMessageLog().logMessage( "Cleaning database " + getStoreDir() + " (" + branchedDataPolicy.name() +
                ") to make way for new db from master" );
        branchedDataPolicy.handle( this );
    }

    private synchronized void startUp( boolean allowInit )
    {
        StoreId storeId = null;
        if ( !new File( getStoreDir(), NeoStore.DEFAULT_NAME ).exists() )
        {   // Try for
            long endTime = System.currentTimeMillis()+60000;
            Exception exception = null;
            while ( System.currentTimeMillis() < endTime )
            {
                // Check if the cluster is up
                Pair<Master, Machine> master = broker.getMasterReally( true );
                if ( master != null && !master.other().equals( Machine.NO_MACHINE ) &&
                        master.other().getMachineId() != machineId )
                {   // Join the existing cluster
                    try
                    {
                        copyStoreFromMaster( master );
                        getMessageLog().logMessage( "copied store from master" );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        master = broker.getMasterReally( true );
                        getMessageLog().logMessage( "Problems copying store from master", e );
                    }
                }
                else
                {   // I seem to be the master, the broker have created the cluster for me
                    // I'm just going to start up now
                    storeId = broker.getClusterStoreId();
                    break;
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
        newMaster( storeId, new Exception( "Starting up for the first time" ) );
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
        getMessageLog().logMessage( "Copying store from master" );
        Response<Void> response = master.first().copyStore( new SlaveContext( 0, machineId, 0, new Pair[0] ),
                new ToFileStoreWriter( getStoreDir() ) );
        long highestLogVersion = highestLogVersion();
        if ( highestLogVersion > -1 ) NeoStore.setVersion( getStoreDir(), highestLogVersion + 1 );
        EmbeddedGraphDatabase copiedDb = new EmbeddedGraphDatabase( getStoreDir(), stringMap( KEEP_LOGICAL_LOGS, "true" ) );
        try
        {
            MasterUtil.applyReceivedTransactions( response, copiedDb, MasterUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
        }
        getMessageLog().logMessage( "Done copying store from master" );
    }

    private long highestLogVersion()
    {
        return XaLogicalLog.getHighestHistoryLogVersion( new File( getStoreDir() ), LOGICAL_LOG_DEFAULT_NAME );
    }

    private EmbeddedGraphDbImpl localGraph()
    {
        if ( localGraph != null ) return localGraph;
        int secondsWait = Math.max( HaConfig.getClientReadTimeoutFromConfig( config )-5, 5 );
        return waitForCondition( new LocalGraphAvailableCondition(), secondsWait*1000 );
    }

    private <T,E extends Exception> T waitForCondition( Condition<T,E> condition, int timeMillis ) throws E
    {
        long endTime = System.currentTimeMillis()+timeMillis;
        T result = condition.tryToFullfill();
        while ( result == null && System.currentTimeMillis() < endTime )
        {
            sleepWithoutInterruption( 1, "Failed waiting for " + condition + " to be fulfilled" );
            result = condition.tryToFullfill();
            if ( result != null ) return result;
        }
        throw condition.failure();
    }

    private BrokerFactory defaultBrokerFactory()
    {
        return new BrokerFactory()
        {
            @Override
            public Broker create( AbstractGraphDatabase graphDb, Map<String, String> config )
            {
                return new ZooKeeperBroker( graphDb, config, HAGraphDb.this );
            }
        };
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
                if ( broker.getMaster() == null
                     && broker instanceof ZooKeeperBroker )
                {
                    /*
                     * Log a message - the ZooKeeperBroker should not return
                     * null master
                     */
                    getMessageLog().logMessage(
                            "ZooKeeper broker returned null master" );
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }
                else if ( broker.getMaster().first() == null )
                {
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }
                receive( broker.getMaster().first().pullUpdates(
                        getSlaveContext( -1 ) ) );
            }
        }
        catch ( ZooKeeperException e )
        {
            newMaster( e );
            throw e;
        }
        catch ( ComException e )
        {
            newMaster( e );
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
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localGraph().getManagementBeans( type );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getStoreDir() + ", " + HaConfig.CONFIG_KEY_SERVER_ID + ":" + machineId + "]";
    }

    /**
     * Shuts down the broker, invalidating every connection to the zookeeper
     * cluster and starts it again. Should be called in case a ConnectionExpired
     * event is received, this is the equivalent of building the ZK connection
     * from start. Also triggers a master reelect, to make sure that the state
     * ZK ended up in during our absence is respected.
     */
    @Override
    public void reconnect( Exception e )
    {
        if ( broker != null )
        {
            broker.shutdown();
        }
        this.broker = brokerFactory.create( this, config );
        newMaster( e );
    }

    protected synchronized void reevaluateMyself( StoreId storeId )
    {
        Pair<Master, Machine> master = broker.getMasterReally( true );
        boolean iAmCurrentlyMaster = masterServer != null;
        getMessageLog().logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                "] (I am master=" + iAmCurrentlyMaster + ", " + localGraph + ")" );
        pullUpdates = false;
        EmbeddedGraphDbImpl newDb = null;
        try
        {
            if ( master.other().getMachineId() == machineId )
            {   // I am the new master
                if ( this.localGraph == null || !iAmCurrentlyMaster )
                {   // I am currently a slave, so restart as master
                    internalShutdown( true );
                    newDb = startAsMaster( storeId );
                }
                // fire rebound event
                broker.rebindMaster();
            }
            else
            {   // Someone else is master
                broker.notifyMasterChange( master.other() );
                if ( this.localGraph == null || iAmCurrentlyMaster )
                {   // I am currently master, so restart as slave.
                    // This will result in clearing of free ids from .id files, see SlaveIdGenerator.
                    internalShutdown( true );
                    newDb = startAsSlave( storeId, master );
                }
                else
                {   // I am already a slave, so just forget the ids I got from the previous master
                    ((SlaveIdGeneratorFactory) getConfig().getIdGeneratorFactory()).forgetIdAllocationsFromMaster();
                }

                ensureDataConsistencyWithMaster( newDb != null ? newDb : localGraph, master );
                getMessageLog().logMessage( "Data consistent with master" );
            }
            if ( newDb != null )
            {
                doAfterLocalGraphStarted( newDb );

                // Assign the db last
                this.localGraph = newDb;

                /*
                 * We have to instantiate the update puller after the local db has been assigned.
                 * Another way to do it is to wait on a LocalGraphAvailableCondition. I chose this,
                 * it is simpler to follow, provided you know what a volatile does.
                 */
                if ( masterServer == null )
                {
                    // The above being true means we are a slave
                    instantiateAutoUpdatePullerIfConfigSaysSo();
                    pullUpdates = true;
                }
            }
        }
        catch ( Throwable t )
        {
            safelyShutdownDb( newDb );
            throw launderedException( t );
        }
    }

    private void safelyShutdownDb( EmbeddedGraphDbImpl newDb )
    {
        try
        {
            if ( newDb != null ) newDb.shutdown();
        }
        catch ( Exception e )
        {
            getMessageLog().logMessage( "Couldn't shut down newly started db", e );
        }
    }

    private void doAfterLocalGraphStarted( EmbeddedGraphDbImpl newDb )
    {
        broker.setConnectionInformation( newDb.getKernelData() );
        for ( TransactionEventHandler<?> handler : transactionEventHandlers )
        {
            newDb.registerTransactionEventHandler( handler );
        }
        for ( KernelEventHandler handler : kernelEventHandlers )
        {
            newDb.registerKernelEventHandler( handler );
        }
    }

    private void logHaInfo( String started )
    {
        getMessageLog().logMessage( started, true );
        getMessageLog().logMessage( "--- HIGH AVAILABILITY CONFIGURATION START ---" );
        broker.logStatus( getMessageLog() );
        getMessageLog().logMessage( "--- HIGH AVAILABILITY CONFIGURATION END ---", true );
    }

    private EmbeddedGraphDbImpl startAsSlave( StoreId storeId,
            Pair<Master, Machine> master )
    {
        getMessageLog().logMessage( "Starting[" + machineId + "] as slave", true );
        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( getStoreDir(), storeId, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxHook( broker, this ),
                slaveUpdateMode.createUpdater( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        // instantiateAutoUpdatePullerIfConfigSaysSo() moved to
        // reevaluateMyself(), after the local db has been assigned
        logHaInfo( "Started as slave" );
        return result;
    }

    private EmbeddedGraphDbImpl startAsMaster( StoreId storeId )
    {
        getMessageLog().logMessage( "Starting[" + machineId + "] as master", true );
        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( getStoreDir(), storeId, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                new MasterTxIdGeneratorFactory( broker ),
                new MasterTxHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
        logHaInfo( "Started as master" );
        return result;
    }

    private void ensureDataConsistencyWithMaster( EmbeddedGraphDbImpl newDb, Pair<Master, Machine> master )
    {
        if ( master.other().getMachineId() == machineId )
        {
            getMessageLog().logMessage( "I am master so cannot consistency check data with master" );
            return;
        }
        else if ( master.first() == null )
        {
            // Temporarily disconnected from ZK
            RuntimeException cause = new RuntimeException( "Unable to get master from ZK" );
            shutdown( cause, false );
            throw cause;
        }

        XaDataSource nioneoDataSource = newDb.getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer,Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            getMessageLog().logMessage( "Logical log file for txId " + myLastCommittedTx +
                " not found, perhaps due to the db being copied from master. Ignoring." );
            return;
        }
        catch ( IOException e )
        {
            getMessageLog().logMessage( "Failed to get master ID for txId " + myLastCommittedTx + ".", e );
            return;
        }
        catch ( Exception e )
        {
            throw new BranchedDataException( "Maybe not branched data, but it could solve it", e );
        }

        long endTime = System.currentTimeMillis()+readTimeout*1000;
        Pair<Integer, Long> mastersMaster = null;
        RuntimeException failure = null;
        while ( mastersMaster == null && System.currentTimeMillis() < endTime )
        {
            try
            {
                mastersMaster = master.first().getMasterIdForCommittedTx(
                        myLastCommittedTx, getStoreId( newDb ) ).response();
            }
            catch ( ComException e )
            {   // Maybe new master isn't up yet... let's wait a little and retry
                failure = e;
                sleepWithoutInterruption( 500, "Failed waiting for next attempt to contact master" );
            }
        }
        if ( mastersMaster == null ) throw failure;

        if ( myMaster.first() != XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER
            && !myMaster.equals( mastersMaster ) )
        {
            String msg = "Branched data, I (machineId:" + machineId + ") think machineId for txId (" +
                    myLastCommittedTx + ") is " + myMaster + ", but master (machineId:" +
                    master.other().getMachineId() + ") says that it's " + mastersMaster;
            getMessageLog().logMessage( msg, true );
            RuntimeException exception = new BranchedDataException( msg );
            safelyShutdownDb( newDb );
            shutdown( exception, false );
            throw exception;
        }
        getMessageLog().logMessage( "Master id for last committed tx ok with highestTxId=" +
            myLastCommittedTx + " with masterId=" + myMaster, true );
    }

    private StoreId getStoreId( EmbeddedGraphDbImpl db )
    {
        XaDataSource ds = db.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        return ((NeoStoreXaDataSource) ds).getStoreId();
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        long pullInterval = HaConfig.getPullIntervalFromConfig( config );
        if ( pullInterval > 0 )
        {
            updatePuller = new ScheduledThreadPoolExecutor( 1 );
            updatePuller.scheduleWithFixedDelay( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        if ( pullUpdates )
                        {
                            pullUpdates();
                        }
                    }
                    catch ( Exception e )
                    {
                        getMessageLog().logMessage( "Pull updates failed", e  );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public TransactionBuilder tx()
    {
        return localGraph().tx();
    }

    @Override
    public Node createNode()
    {
        return localGraph().createNode();
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

    public synchronized void internalShutdown( boolean rotateLogs )
    {
        getMessageLog().logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this + ", masterServer=" + masterServer, new Exception( "Internal shutdown" ), true );
        pullUpdates = false;
        if ( this.updatePuller != null )
        {
            getMessageLog().logMessage( "Internal shutdown updatePuller", true );
            try
            {
                /*
                 * Be gentle, interrupting running threads could leave the
                 * file channels in a bad shape.
                 */
                this.updatePuller.shutdown();
                this.updatePuller.awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                getMessageLog().logMessage(
                        "Got exception while waiting for update puller termination",
                        e, true );
            }
            getMessageLog().logMessage( "Internal shutdown updatePuller DONE",
                    true );
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            getMessageLog().logMessage( "Internal shutdown masterServer", true );
            this.masterServer.shutdown();
            getMessageLog().logMessage( "Internal shutdown masterServer DONE", true );
            this.masterServer = null;
        }
        if ( this.localGraph != null )
        {
            if ( rotateLogs )
            {
                for ( XaDataSource dataSource : getConfig().getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
                {
                    try
                    {
                        dataSource.rotateLogicalLog();
                    }
                    catch ( IOException e )
                    {
                        getMessageLog().logMessage( "Couldn't rotate logical log for " + dataSource.getName(), e );
                    }
                }
            }
            getMessageLog().logMessage( "Internal shutdown localGraph", true );
            this.localGraph.shutdown();
            getMessageLog().logMessage( "Internal shutdown localGraph DONE", true );
            this.localGraph = null;
        }
        getMessageLog().flush();
    }

    private synchronized void shutdown( Throwable cause, boolean shutdownBroker )
    {
        causeOfShutdown = cause;
        getMessageLog().logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( shutdownBroker && this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown( false );
    }

    @Override
    protected synchronized void close()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
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
        // Constructs a slave context from scratch.
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
            newMaster( e );
            throw new RuntimeException( e );
        }
    }

    @Override
    public void newMaster( Exception e )
    {
        newMaster( null, e );
    }

    private synchronized void newMaster( StoreId storeId, Exception e )
    {
        try
        {
            doNewMaster( storeId, e );
        }
        catch ( BranchedDataException bde )
        {
            getMessageLog().logMessage( "Branched data occured, retrying" );
            getFreshDatabaseFromMaster();
            doNewMaster( storeId, bde );
        }
    }

    private void doNewMaster( StoreId storeId, Exception e )
    {
        try
        {
            getMessageLog().logMessage( "newMaster called", true );
            reevaluateMyself( storeId );
        }
        catch ( ZooKeeperException ee )
        {
            getMessageLog().logMessage( "ZooKeeper exception in newMaster", ee );
            throw Exceptions.launderedException( ee );
        }
        catch ( ComException ee )
        {
            getMessageLog().logMessage( "Communication exception in newMaster", ee );
            throw Exceptions.launderedException( ee );
        }
        catch ( BranchedDataException ee )
        {
            throw ee;
        }
        // BranchedDataException will escape from this method since the catch clause below
        // sees to that.
        catch ( Throwable t )
        {
            getMessageLog().logMessage( "Reevaluation ended in unknown exception " + t
                    + " so shutting down", t, true );
            shutdown( t, false );
            throw Exceptions.launderedException( t );
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
    public IndexManager index()
    {
        return localGraph().index();
    }

    // Only for testing purposes, simulates a network outage almost
    public void shutdownBroker()
    {
        this.broker.shutdown();
    }

    @Override
    public KernelData getKernelData()
    {
        return localGraph().getKernelData();
    }

    enum BranchedDataPolicy
    {
        keep_all
        {
            @Override
            void handle( HAGraphDb db )
            {
                moveAwayDb( db, branchedDataDir( db ) );
            }
        },
        keep_last
        {
            @Override
            void handle( HAGraphDb db )
            {
                File branchedDataDir = branchedDataDir( db );
                moveAwayDb( db, branchedDataDir );
                for ( File file : new File( db.getStoreDir() ).listFiles() )
                {
                    if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                    {
                        try
                        {
                            FileUtils.deleteRecursively( file );
                        }
                        catch ( IOException e )
                        {
                            db.getMessageLog().logMessage( "Couldn't delete old branched data directory " + file, e );
                        }
                    }
                }
            }
        },
        keep_none
        {
            @Override
            void handle( HAGraphDb db )
            {
                for ( File file : relevantDbFiles( db ) )
                {
                    try
                    {
                        FileUtils.deleteRecursively( file );
                    }
                    catch ( IOException e )
                    {
                        db.getMessageLog().logMessage( "Couldn't delete file " + file, e );
                    }
                }
            }
        },
        shutdown
        {
            @Override
            void handle( HAGraphDb db )
            {
                db.shutdown();
            }
        };

        static String BRANCH_PREFIX = "branched-";

        abstract void handle( HAGraphDb db );

        protected void moveAwayDb( HAGraphDb db, File branchedDataDir )
        {
            for ( File file : relevantDbFiles( db ) )
            {
                File dest = new File( branchedDataDir, file.getName() );
                if ( !file.renameTo( dest ) ) db.getMessageLog().logMessage( "Couldn't move " + file.getPath() );
            }
        }

        File branchedDataDir( HAGraphDb db )
        {
            File result = new File( db.getStoreDir(), BRANCH_PREFIX + System.currentTimeMillis() );
            result.mkdirs();
            return result;
        }

        File[] relevantDbFiles( HAGraphDb db )
        {
            return new File( db.getStoreDir() ).listFiles( new FileFilter()
            {
                @Override
                public boolean accept( File file )
                {
                    return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !isBranchedDataDirectory( file );
                }
            } );
        }

        boolean isBranchedDataDirectory( File file )
        {
            return file.isDirectory() && file.getName().startsWith( BRANCH_PREFIX );
        }
    }

    private interface Condition<T, E extends Exception>
    {
        T tryToFullfill();

        E failure();
    }

    private class LocalGraphAvailableCondition implements Condition<EmbeddedGraphDbImpl, RuntimeException>
    {
        @Override
        public EmbeddedGraphDbImpl tryToFullfill()
        {
            return localGraph;
        }

        public RuntimeException failure()
        {
            if ( causeOfShutdown != null )
            {
                return new RuntimeException( "Graph database not started", causeOfShutdown );
            }
            else
            {
                return new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                        "maybe not started yet or in the middle of master/slave swap?" );
            }
        }
    }
}
