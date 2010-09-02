package org.neo4j.kernel;

import java.io.File;
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

import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Pair;
import org.neo4j.index.IndexService;
import org.neo4j.index.impl.lucene.LuceneIndexProvider;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.HaCommunicationException;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
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
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

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
    private volatile IndexService localIndexService;
    private volatile IndexProvider localIndexProvider;
    private final int machineId;
    private volatile MasterServer masterServer;
    private final AtomicBoolean reevaluatingMyself = new AtomicBoolean();
    private ScheduledExecutorService updatePuller;
    
    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    // Just "cached" instances which are used internally here
    private XaDataSourceManager localDataSourceManager;
    
    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
//        config = makeDbCopyIfNoRecreationStrategySpecified( storeDir, config );
        this.storeDir = storeDir;
        this.config = config;
        assertIWasntMasterWhenShutDown();
        this.brokerFactory = defaultBrokerFactory( storeDir, config );
        this.machineId = getMachineIdFromConfig( config );
        this.broker = brokerFactory.create( storeDir, config );
        reevaluateMyself();
    }

//    /**
//     * This is juuust for small data sets and should only be used when testing
//     * it out. Ease of use on a n00b level.
//     */
//    private Map<String, String> makeDbCopyIfNoRecreationStrategySpecified( String storeDir,
//            Map<String, String> config )
//    {
//        if ( config.containsKey( CONFIG_KEY_HA_SKELETON_DB_PATH ) ||
//                !new File( storeDir ).exists() )
//        {
//            return config;
//        }
//        
//        try
//        {
//            File tmpFile = File.createTempFile( "test", "test" );
//            File tmpDir = tmpFile.getParentFile();
//            tmpFile.delete();
//            
//            File tmpDbDir = new File( tmpDir, "hadb-" + System.currentTimeMillis() );
//            FileUtils.copyDirectory( new File( storeDir ), tmpDbDir );
//            Map<String, String> result = new HashMap<String, String>( config );
//            result.put( CONFIG_KEY_HA_SKELETON_DB_PATH, tmpDbDir.getAbsolutePath() );
//            System.out.println( "Made a tmp copy to " + tmpDbDir.getAbsolutePath() );
//            return result;
//        }
//        catch ( IOException e )
//        {
//            throw new RuntimeException( e );
//        }
//    }

    /**
     * Only for testing
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.brokerFactory = brokerFactory;
        this.machineId = getMachineIdFromConfig( config );
        this.broker = brokerFactory.create( storeDir, config );
        reevaluateMyself();
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
            receive( broker.getMaster().pullUpdates( getSlaveContext( -1 ) ) );
        }
        catch ( ZooKeeperException e )
        {
            somethingIsWrong( e );
            throw e;
        }
        catch ( HaCommunicationException e )
        {
            somethingIsWrong( e );
            throw e;
        }
    }
    
    public Config getConfig()
    {
        return this.localGraph.getConfig();
    }
    
    public String getStoreDir()
    {
        return this.storeDir;
    }
    
    @Override
    public <T> T getManagementBean( Class<T> type )
    {
        return this.localGraph.getManagementBean( type );
    }
    
    protected void reevaluateMyself()
    {
        if ( !reevaluatingMyself.compareAndSet( false, true ) )
        {
            return;
        }
                
        try
        {
            broker.invalidateMaster();
            boolean brokerSaysIAmMaster = brokerSaysIAmMaster();
            boolean iAmCurrentlyMaster = masterServer != null;
            boolean restarted = false;
            if ( brokerSaysIAmMaster )
            {
                if ( !iAmCurrentlyMaster )
                {
                    internalShutdown();
                }
                if ( this.localGraph == null )
                {
                    startAsMaster();
                    restarted = true;
                }
            }
            else
            {
                if ( iAmCurrentlyMaster )
                {
                    internalShutdown();
                }
                if ( this.localGraph == null )
                {
                    startAsSlave();
                    restarted = true;
                }
            }
            
            if ( restarted )
            {
                dbGotRestarted();
            }
        }
        finally
        {
            reevaluatingMyself.set( false );
        }
    }

    private void startAsSlave()
    {
        assertIWasntMasterWhenShutDown();
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxRollbackHook( broker, this ),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
        graphDbStarted();
        instantiateIndexIfNeeded();
        instantiateAutoUpdatePullerIfConfigSaysSo();
    }

    private void graphDbStarted()
    {
        for ( TransactionEventHandler<?> handler : transactionEventHandlers )
        {
            this.localGraph.registerTransactionEventHandler( handler );
        }
        for ( KernelEventHandler handler : kernelEventHandlers )
        {
            this.localGraph.registerKernelEventHandler( handler );
        }
    }

    private void startAsMaster()
    {
        assertIWasntMasterWhenShutDown();
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxFinishHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
        graphDbStarted();
        markThatIAmMaster();
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
        instantiateIndexIfNeeded();
    }
    
    private File getMasterMarkFile()
    {
        return new File( storeDir, "i-am-master" );
    }

    private void markThatIAmMaster()
    {
        File file = getMasterMarkFile();
        try
        {
            file.createNewFile();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private void assertIWasntMasterWhenShutDown()
    {
        if ( getMasterMarkFile().exists() )
        {
            if ( !recreateDb() )
            {
                throw new RuntimeException( "I was master the previous session, " +
                        "so can't start up in this state (and no method specified how " +
                        "I should replicate from another DB" ); 
            }
        }
    }
    
    private boolean recreateDb()
    {
        // This is temporary and shouldn't be used in production, but the
        // functionality is the same: I come to the conclusion that this db
        // is void and should be recreated from some source.
        String recreateFrom = this.config.get( CONFIG_KEY_HA_SKELETON_DB_PATH );
        if ( recreateFrom != null )
        {
            try
            {
                FileUtils.cleanDirectory( new File( storeDir ) );
                FileUtils.copyDirectory( new File( recreateFrom ), new File( storeDir ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            System.out.println( "=== RECREATED DB from " + recreateFrom + " ===" );
            return true;
        }
        return false;
    }

    private void dbGotRestarted()
    {
        this.localDataSourceManager =
            localGraph.getConfig().getTxModule().getXaDataSourceManager();
        NeoStoreUtil store = new NeoStoreUtil( storeDir );
        broker.setLastCommittedTxId( store.getLastCommittedTx() );
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
                    pullUpdates();
                }
            }, timeMillis, timeMillis, TimeUnit.MILLISECONDS );
        }
    }
    
    // This whole thing with instantiating indexes internally depending on config
    // is obviously temporary
    private void instantiateIndexIfNeeded()
    {
        if ( Boolean.parseBoolean( config.get( "index" ) ) )
        {
            this.localIndexService = new LuceneIndexService( this );
            this.localIndexProvider = new LuceneIndexProvider( this );
        }
    }

    private boolean brokerSaysIAmMaster()
    {
        return broker.thisIsMaster();
    }

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
        System.out.println( "Internal shutdown of HA db " + this );
        if ( this.updatePuller != null )
        {
            this.updatePuller.shutdown();
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            this.masterServer.shutdown();
            this.masterServer = null;
        }
        if ( this.localIndexService != null )
        {
            this.localIndexService.shutdown();
            this.localIndexService = null;
            this.localIndexProvider = null;
        }
        if ( this.localGraph != null )
        {
            this.localGraph.shutdown();
            this.localGraph = null;
            this.localDataSourceManager = null;
        }
    }
    
    public synchronized void shutdown()
    {
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
            somethingIsWrong( e );
            throw new RuntimeException( e );
        }
    }
    
    public void somethingIsWrong( Exception e )
    {
        e.printStackTrace();
        new Thread()
        {
            @Override
            public void run()
            {
                for ( int i = 0; i < 5; i++ )
                {
                    try
                    {
                        reevaluateMyself();
                        break;
                    }
                    catch ( ZooKeeperException e )
                    {
                        e.printStackTrace();
                    }
                    catch ( HaCommunicationException e )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }
    
    public IndexService getIndexService()
    {
        return this.localIndexService;
    }
    
    public IndexProvider getIndexProvider()
    {
        return this.localIndexProvider;
    }
    
    protected MasterServer getMasterServerIfMaster()
    {
        return masterServer;
    }
}
