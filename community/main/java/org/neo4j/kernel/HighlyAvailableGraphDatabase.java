/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.HaCommunicationException;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.MasterTxIdGenerator.MasterTxIdGeneratorFactory;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveTxRollbackHook;
import org.neo4j.kernel.ha.TransactionStream;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class HighlyAvailableGraphDatabase extends AbstractGraphDatabase
        implements GraphDatabaseService, ResponseReceiver
{
    public static final String CONFIG_KEY_HA_MACHINE_ID = "ha.machine_id";
    public static final String CONFIG_KEY_HA_ZOO_KEEPER_SERVERS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_HA_SERVER = "ha.server";
    public static final String CONFIG_KEY_HA_PULL_INTERVAL = "ha.pull_interval";

    // Temporary name
    public static final String CONFIG_KEY_HA_SKELETON_DB_PATH = "ha.skeleton_db_path";

    private final String storeDir;
    private final Map<String, String> config;
    private final BrokerFactory brokerFactory;
    private final Broker broker;
    private volatile EmbeddedGraphDbImpl localGraph;
//    private volatile IndexService localIndexService;
//    private volatile IndexProvider localIndexProvider;
    private final int machineId;
    private volatile MasterServer masterServer;
    private final AtomicBoolean reevaluatingMyself = new AtomicBoolean();
    private ScheduledExecutorService updatePuller;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    // Just "cached" instances which are used internally here
//    private XaDataSourceManager localDataSourceManager;

    private final StringLogger msgLog;

    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        if ( config == null )
        {
            throw new IllegalArgumentException( "null config, proper configuration required" );
        }
        this.storeDir = storeDir;
        this.config = config;
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        this.brokerFactory = defaultBrokerFactory( storeDir, config );
        this.machineId = getMachineIdFromConfig( config );
        this.broker = brokerFactory.create( storeDir, config );
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        startUp();
    }

    /**
     * Only for testing
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory )
    {
        this.storeDir = storeDir;
        this.config = config;
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        this.brokerFactory = brokerFactory;
        this.machineId = getMachineIdFromConfig( config );
        this.broker = brokerFactory.create( storeDir, config );
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        startUp();
    }

    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDatabase.loadConfigurations( file );
    }

    private void startUp()
    {
        newMaster( null, new Exception() );
        if ( localGraph == null )
        {
            throw new RuntimeException( "Failed to find a master" );
        }
    }

    private BrokerFactory defaultBrokerFactory( final String storeDir,
            final Map<String, String> config )
    {
        return new BrokerFactory()
        {
            public Broker create( String storeDir, Map<String, String> config )
            {
                return new ZooKeeperBroker( storeDir,
                        getMachineIdFromConfig( config ),
                        getZooKeeperServersFromConfig( config ),
                        getHaServerFromConfig( config ), HighlyAvailableGraphDatabase.this );
            }
        };
    }

    private static String getHaServerFromConfig( Map<?, ?> config )
    {
        return (String) config.get( CONFIG_KEY_HA_SERVER );
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
        catch ( HaCommunicationException e )
        {
            newMaster( null, e );
            throw e;
        }
    }

    @Override
    public Config getConfig()
    {
        return this.localGraph.getConfig();
    }

    @Override
    public String getStoreDir()
    {
        return this.storeDir;
    }

    @Override
    public <T> T getManagementBean( Class<T> type )
    {
        return this.localGraph.getManagementBean( type );
    }

    protected synchronized void reevaluateMyself( Pair<Master, Machine> master )
    {
//        if ( !reevaluatingMyself.compareAndSet( false, true ) )
//        {
//            return;
//        }
//        try
//        {
        if ( master == null )
        {
            master = broker.getMasterReally();
        }

        boolean restarted = false;
        boolean iAmCurrentlyMaster = masterServer != null;
        msgLog.logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                "] (I am master=" + iAmCurrentlyMaster + ")" );
        if ( master.other().getMachineId() == machineId )
        {
            // I am master
            if ( this.localGraph == null || !iAmCurrentlyMaster )
            {
                internalShutdown();
                startAsMaster();
                restarted = true;
            }
            // fire rebound event
            broker.rebindMaster();
        }
        else
        {
            if ( this.localGraph == null || iAmCurrentlyMaster )
            {
                internalShutdown();
                startAsSlave();
                restarted = true;
            }
            tryToEnsureIAmNotABrokenMachine( broker.getMaster() );
        }

        if ( restarted )
        {
            for ( TransactionEventHandler<?> handler : transactionEventHandlers )
            {
                this.localGraph.registerTransactionEventHandler( handler );
            }
            for ( KernelEventHandler handler : kernelEventHandlers )
            {
                this.localGraph.registerKernelEventHandler( handler );
            }
//            this.localDataSourceManager =
//                    localGraph.getConfig().getTxModule().getXaDataSourceManager();
        }
//        }
//        finally
//        {
//            reevaluatingMyself.set( false );
//        }
    }

    private void startAsSlave()
    {
        msgLog.logMessage( "Starting[" + machineId + "] as slave", true );
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxRollbackHook( broker, this ),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
//        instantiateIndexIfNeeded();
        instantiateAutoUpdatePullerIfConfigSaysSo();
        msgLog.logMessage( "Started as slave", true );
    }

    private void startAsMaster()
    {
        msgLog.logMessage( "Starting[" + machineId + "] as master", true );
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                new MasterTxIdGeneratorFactory( broker ),
                CommonFactories.defaultTxFinishHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
//        instantiateIndexIfNeeded();
        msgLog.logMessage( "Started as master", true );
    }

    private void tryToEnsureIAmNotABrokenMachine( Pair<Master, Machine> master )
    {
        try
        {
            if ( master.other().getMachineId() == machineId )
            {
                return;
            }

            XaDataSource nioneoDataSource = this.localGraph.getConfig().getTxModule()
                    .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
            long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
            long highestCommonTxId = Math.min( myLastCommittedTx, master.other().getLastCommittedTxId() );
            int masterForMyHighestCommonTxId = nioneoDataSource.getMasterForCommittedTx( highestCommonTxId );
            int masterForMastersHighestCommonTxId = master.first().getMasterIdForCommittedTx( highestCommonTxId );

            // Compare those two, if equal -> good
            if ( masterForMyHighestCommonTxId == masterForMastersHighestCommonTxId )
            {
                msgLog.logMessage( "Master id for last committed tx ok with highestCommonTxId=" + 
                        highestCommonTxId + " with masterId=" + masterForMyHighestCommonTxId, true );
                return;
            }
            else
            {
                String msg = "Broken store, my last committed tx,machineId[" +
                    myLastCommittedTx + "," + masterForMyHighestCommonTxId +
                    "] but master says machine id for that txId is " + masterForMastersHighestCommonTxId;
                msgLog.logMessage( msg, true );
                shutdown();
                throw new RuntimeException( msg );
            }
        }
        catch ( IOException e )
        {
            shutdown();
            throw new RuntimeException( e );
        }
    }

    private boolean recreateDbSomehow()
    {
        // This is temporary and shouldn't be used in production, but the
        // functionality is the same: I come to the conclusion that this db
        // is void and should be recreated from some source.
//        String recreateFrom = this.config.get( CONFIG_KEY_HA_SKELETON_DB_PATH );
//        if ( recreateFrom != null )
//        {
//            try
//            {
//                FileUtils.cleanDirectory( new File( storeDir ) );
//                FileUtils.copyDirectory( new File( recreateFrom ), new File( storeDir ) );
//            }
//            catch ( IOException e )
//            {
//                throw new RuntimeException( e );
//            }
//            msgLog.logMessage( "=== RECREATED DB from " + recreateFrom + " ===" );
//            return true;
//        }
//        return false;
        throw new UnsupportedOperationException();
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

    // This whole thing with instantiating indexes internally depending on config
    // is obviously temporary
//    private void instantiateIndexIfNeeded()
//    {
//        if ( Boolean.parseBoolean( config.get( "index" ) ) )
//        {
////            this.localIndexService = new LuceneIndexService( this );
//            this.localIndexProvider = new LuceneIndexProvider( this );
//        }
//    }

    public Transaction beginTx()
    {
        return localGraph.beginTx();
    }

    public Node createNode()
    {
        return localGraph.createNode();
    }

    public boolean enableRemoteShell()
    {
        return localGraph.enableRemoteShell();
    }

    public boolean enableRemoteShell( Map<String, Serializable> initialProperties )
    {
        return localGraph.enableRemoteShell( initialProperties );
    }

    public Iterable<Node> getAllNodes()
    {
        return localGraph.getAllNodes();
    }

    public Node getNodeById( long id )
    {
        return localGraph.getNodeById( id );
    }

    public Node getReferenceNode()
    {
        return localGraph.getReferenceNode();
    }

    public Relationship getRelationshipById( long id )
    {
        return localGraph.getRelationshipById( id );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return localGraph.getRelationshipTypes();
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        this.kernelEventHandlers.add( handler );
        return localGraph.registerKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return localGraph.registerTransactionEventHandler( handler );
    }

    public synchronized void internalShutdown()
    {
        msgLog.logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this, true );
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
//        if ( this.localIndexService != null )
//        {
//            msgLog.logMessage( "Internal shutdown index" );
//            this.localIndexService.shutdown();
//            msgLog.logMessage( "Internal shutdown index DONE" );
//            this.localIndexService = null;
//            this.localIndexProvider = null;
//        }
        if ( this.localGraph != null )
        {
            msgLog.logMessage( "Internal shutdown localGraph", true );
            this.localGraph.shutdown();
            msgLog.logMessage( "Internal shutdown localGraph DONE", true );
            this.localGraph = null;
//            this.localDataSourceManager = null;
        }
    }

    public synchronized void shutdown()
    {
        msgLog.logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown();
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return localGraph.unregisterKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return localGraph.unregisterTransactionEventHandler( handler );
    }

    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        XaDataSourceManager localDataSourceManager =
            localGraph.getConfig().getTxModule().getXaDataSourceManager();
        Collection<XaDataSource> dataSources = localDataSourceManager.getAllRegisteredDataSources();
        @SuppressWarnings("unchecked")
        Pair<String, Long>[] txs = new Pair[dataSources.size()];
        int i = 0;
        for ( XaDataSource dataSource : dataSources )
        {
            txs[i++] = new Pair<String, Long>(
                    dataSource.getName(), dataSource.getLastCommittedTxId() );
        }
        return new SlaveContext( machineId, eventIdentifier, txs );
    }

    public <T> T receive( Response<T> response )
    {
        try
        {
            XaDataSourceManager localDataSourceManager =
                localGraph.getConfig().getTxModule().getXaDataSourceManager();
            for ( Pair<String, TransactionStream> streams : response.transactions().getStreams() )
            {
                String resourceName = streams.first();
                XaDataSource dataSource = localDataSourceManager.getXaDataSource( resourceName );
                for ( Pair<Long, ReadableByteChannel> channel : streams.other().getChannels() )
                {
                    dataSource.applyCommittedTransaction( channel.first(), channel.other() );
                    channel.other().close();
                }
            }
            return response.response();
        }
        catch ( IOException e )
        {
            newMaster( broker.getMaster(), e );
            throw new RuntimeException( e );
        }
    }

    public void newMaster( Pair<Master, Machine> master, Exception e )
    {
        try
        {
            msgLog.logMessage( "newMaster( " + master + ") called", e, true );
            reevaluateMyself( master );
        }
        catch ( ZooKeeperException ee )
        {
            ee.printStackTrace();
        }
        catch ( HaCommunicationException ee )
        {
            ee.printStackTrace();
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            msgLog.logMessage( "Reevaluation ended in unknown exception " + t
                    + " so shutting down", true );
            shutdown();
        }
    }

//    public IndexService getIndexService()
//    {
//        return this.localIndexService;
//    }

//    public IndexProvider getIndexProvider()
//    {
//        return this.localIndexProvider;
//    }

    protected MasterServer getMasterServerIfMaster()
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
    
    public IndexManager index()
    {
        return this.localGraph.index();
    }
}