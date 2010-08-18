package org.neo4j.kernel;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

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
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService, ResponseReceiver
{
    public static final String CONFIG_KEY_HA_MACHINE_ID = "ha.machine_id";
    public static final String CONFIG_KEY_HA_ZOO_KEEPER_SERVERS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_HA_SERVERS = "ha.servers";
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
    private UpdatePuller updatePuller;
    
    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this( storeDir, config, defaultBrokerFactory( storeDir, config ) );
    }

    private static BrokerFactory defaultBrokerFactory( final String storeDir,
            final Map<String, String> config )
    {
        return new BrokerFactory()
        {
            public Broker create()
            {
                return instantiateBroker( storeDir, config );
            }
        };
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
        this.broker = brokerFactory.create();
        reevaluateMyself();
    }
    
    private static Broker instantiateBroker( String storeDir, Map<String, String> config )
    {
        return new ZooKeeperBroker( storeDir,
                getMachineIdFromConfig( config ),
                getZooKeeperServersFromConfig( config ),
                getHaServersFromConfig( config ) );
    }
    
    private static Map<Integer, String> getHaServersFromConfig(
            Map<?, ?> config )
    {
        String value = config.get( CONFIG_KEY_HA_SERVERS ).toString();
        Map<Integer, String> result = new HashMap<Integer, String>();
        for ( String part : value.split( Pattern.quote( "," ) ) )
        {
            String[] tokens = part.trim().split( Pattern.quote( "=" ) );
            result.put( new Integer( tokens[0] ), tokens[1] );
        }
        return result;
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
            System.out.println( Thread.currentThread().getName() + " pulls updates" );
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
        return localGraph.getConfig();
    }
    
    protected void reevaluateMyself()
    {
        if ( !reevaluatingMyself.compareAndSet( false, true ) )
        {
            return;
        }
                
        try
        {
            boolean brokerSaysIAmMaster = brokerSaysIAmMaster();
            boolean iAmCurrentlyMaster = masterServer != null;
            if ( brokerSaysIAmMaster )
            {
                if ( !iAmCurrentlyMaster )
                {
                    shutdown();
                }
                if ( this.localGraph == null )
                {
                    startAsSlave();
                }
            }
            else
            {
                if ( iAmCurrentlyMaster )
                {
                    shutdown();
                }
                if ( this.localGraph == null )
                {
                    startAsMaster();
                }
            }
        }
        finally
        {
            reevaluatingMyself.set( false );
        }
    }

    private void startAsMaster()
    {
        this.broker = brokerFactory.create();
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxRollbackHook( broker, this ),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
        instantiateIndexIfNeeded();
    }

    private void startAsSlave()
    {
        this.broker = brokerFactory.create();
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
        this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxRollbackHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ) );
        instantiateIndexIfNeeded();
        instantiateAutoUpdatePullerIfConfigSaysSo();
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        String pullInterval = this.config.get( CONFIG_KEY_HA_PULL_INTERVAL );
        if ( pullInterval != null )
        {
            updatePuller = UpdatePuller.startAutoPull( this,
                    TimeUtil.parseTimeMillis( pullInterval ) );
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

    public synchronized void shutdown()
    {
        if ( this.updatePuller != null )
        {
            System.out.println( "Shutting down update puller" );
            this.updatePuller.halt();
            this.updatePuller = null;
        }
        if ( this.broker != null )
        {
            System.out.println( "Shutting down broker" );
            this.broker.shutdown();
            this.broker = null;
        }
        if ( this.masterServer != null )
        {
            System.out.println( "Shutting down master server" );
            this.masterServer.shutdown();
            this.masterServer = null;
        }
        if ( this.localIndex != null )
        {
            System.out.println( "Shutting down local index" );
            this.localIndex.shutdown();
            this.localIndex = null;
        }
        if ( this.localGraph != null )
        {
            System.out.println( "Shutting down local graph" );
            this.localGraph.shutdown();
            this.localGraph = null;
        }
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
        Config config = getConfig();
        Map<String, Long> txs = new HashMap<String, Long>();
        for ( XaDataSource dataSource :
                config.getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            txs.put( dataSource.getName(), dataSource.getLastCommittedTxId() );
        }
        System.out.println( "Sending slaveContext:" + machineId + ", " + txs );
        return new SlaveContext( machineId, txs );
    }

    public <T> T receive( Response<T> response )
    {
        try
        {
            for ( Pair<String, TransactionStream> streams : response.transactions().getStreams() )
            {
                String resourceName = streams.first();
                XaDataSource dataSource = localGraph.getConfig().getTxModule().getXaDataSourceManager()
                        .getXaDataSource( resourceName );
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
}
