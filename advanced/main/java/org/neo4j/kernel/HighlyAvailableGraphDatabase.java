package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Map;
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
import org.neo4j.helpers.Pair;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.HaCommunicationException;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveTxRollbackHook;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.ha.SlaveContext;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService, ResponseReceiver
{
    public static final String CONFIG_KEY_HA_MACHINE_ID = "ha.machine_id";
    public static final String CONFIG_KEY_HA_ZOO_KEEPER_SERVERS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_HA_SERVER = "ha.server";
    public static final String CONFIG_KEY_HA_PULL_INTERVAL = "ha.pull_interval";
    
    private final String storeDir;
    private final Map<String, String> config;
    private final BrokerFactory brokerFactory;
    private Broker broker;
    private EmbeddedGraphDbImpl localGraph;
    private IndexService localIndex;
    private final int machineId;
    private MasterServer masterServer;
    private AtomicBoolean reevaluatingMyself = new AtomicBoolean();
    private ScheduledExecutorService updatePuller;

    // Just "cached" instances which are used internally here
    private XaDataSourceManager localDataSourceManager;
    
    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this.storeDir = storeDir;
        assertIWasntMasterWhenShutDown();
        this.config = config;
        this.brokerFactory = defaultBrokerFactory( storeDir, config );
        this.machineId = getMachineIdFromConfig( config );
        this.broker = brokerFactory.create( storeDir, config );
        reevaluateMyself();
    }

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
            receive( broker.getMaster().pullUpdates( getSlaveContext() ) );
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
            if ( brokerSaysIAmMaster )
            {
                if ( !iAmCurrentlyMaster )
                {
                    internalShutdown();
                }
                if ( this.localGraph == null )
                {
                    startAsMaster();
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
                }
            }
            this.localDataSourceManager =
                    localGraph.getConfig().getTxModule().getXaDataSourceManager();
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
        instantiateIndexIfNeeded();
        instantiateAutoUpdatePullerIfConfigSaysSo();
    }

    private void startAsMaster()
    {
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxRollbackHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
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
            throw new RuntimeException( "I was master the previous session, " +
                    "so can't start up in this state" );
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
                public void run()
                {
                    pullUpdates();
                }
            }, timeMillis, timeMillis, TimeUnit.MILLISECONDS );
        }
    }
    
    private void instantiateIndexIfNeeded()
    {
        if ( Boolean.parseBoolean( config.get( "index" ) ) )
        {
            this.localIndex = new LuceneIndexService( this );
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
        return localGraph.registerKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
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
        if ( this.localIndex != null )
        {
            this.localIndex.shutdown();
            this.localIndex = null;
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
            this.broker = null;
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
    
    public SlaveContext getSlaveContext()
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
        return new SlaveContext( machineId, txs );
    }

    public <T> T receive( Response<T> response )
    {
        try
        {
            for ( Pair<String, TransactionStream> streams : response.transactions().getStreams() )
            {
                String resourceName = streams.first();
                XaDataSource dataSource = localDataSourceManager.getXaDataSource( resourceName );
                for ( ReadableByteChannel channel : streams.other().getChannels() )
                {
                    dataSource.applyCommittedTransaction( channel );
                    channel.close();
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
        new Thread()
        {
            @Override
            public void run()
            {
                reevaluateMyself();
            }
        }.start();
    }
    
    public IndexService getIndexService()
    {
        return this.localIndex;
    }
    
    protected MasterServer getMasterServerIfMaster()
    {
        return masterServer;
    }
}
