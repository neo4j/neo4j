package org.neo4j.kernel;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveTxRollbackHook;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
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
    
    private final String storeDir;
    private final Map<String, String> config;
    private final Broker broker;
    private EmbeddedGraphDbImpl localGraph;
    private final int machineId;
    private MasterServer masterServer;

    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this( storeDir, config, instantiateBroker( storeDir, config ) );
    }
    
    /**
     * Only for testing
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config,
            Broker broker )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.broker = broker;
        this.machineId = getMachineIdFromConfig( config );
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
        receive( broker.getMaster().pullUpdates( getSlaveContext() ) );
    }

//    private Broker instantiateBroker()
//    {
//        String cls = config.get( "ha_broker" );
//        cls = cls != null ? cls : "some.default.Broker";
//        try
//        {
//            return Class.forName( cls ).asSubclass( Broker.class ).getConstructor(
//                    Map.class ).newInstance( config );
//        }
//        catch ( Exception e )
//        {
//            throw new RuntimeException( e );
//        }
//    }
    
    public Config getConfig()
    {
        return localGraph.getConfig();
    }
    
    protected void reevaluateMyself()
    {
        boolean brokerSaysIAmMaster = brokerSaysIAmMaster();
        boolean iAmCurrentlyMaster = masterServer != null;
        if ( brokerSaysIAmMaster )
        {
            if ( !iAmCurrentlyMaster )
            {
                shutdownIfStarted();
            }
            this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    CommonFactories.defaultLockManagerFactory(),
                    CommonFactories.defaultIdGeneratorFactory(),
                    CommonFactories.defaultRelationshipTypeCreator(),
                    CommonFactories.defaultTxIdGeneratorFactory(),
                    CommonFactories.defaultTxRollbackHook() );
        }
        else
        {
            if ( iAmCurrentlyMaster )
            {
                shutdownIfStarted();
            }
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    new SlaveLockManagerFactory( broker, this ),
                    new SlaveIdGeneratorFactory( broker, this ),
                    new SlaveRelationshipTypeCreator( broker, this ),
                    new SlaveTxIdGeneratorFactory( broker, this ),
                    new SlaveTxRollbackHook( broker, this ) );
        }
    }

    private int readHaMasterListenPort()
    {
        Map<Integer, String> haServers = getHaServersFromConfig( config );
        System.out.println( haServers );
        int machineId = broker.getMyMachineId();
        System.out.println( "machineId:" + machineId );
        String host = haServers.get( machineId );
        return Integer.parseInt( host.split( Pattern.quote( ":" ) )[1] );
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

    public void shutdown()
    {
        shutdownIfStarted();
    }

    private void shutdownIfStarted()
    {
        if ( this.masterServer != null )
        {
            this.masterServer.shutdown();
            this.masterServer = null;
        }
        if ( this.localGraph != null )
        {
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
                }
            }
            return response.response();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
