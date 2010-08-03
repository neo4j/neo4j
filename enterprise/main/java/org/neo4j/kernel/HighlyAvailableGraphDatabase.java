package org.neo4j.kernel;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService, ResponseReceiver
{
    private final String storeDir;
    private final Map<String, String> config;
    private final Broker broker;
    private EmbeddedGraphDbImpl localGraph;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config, Broker broker )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.broker = broker;
//        this.broker = instantiateBroker();
        reevaluateMyself();
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
        shutdownIfNecessary();
        if ( brokerSaysIAmMaster() )
        {
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    LockManagerFactory.DEFAULT, IdGeneratorFactory.DEFAULT,
                    DefaultRelationshipTypeCreator.INSTANCE, TxIdGeneratorFactory.DEFAULT );
        }
        else
        {
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    new SlaveLockManagerFactory( broker, this ),
                    new SlaveIdGeneratorFactory( broker, this ),
                    new SlaveRelationshipTypeCreator( broker, this ),
                    new SlaveTxIdGeneratorFactory( broker, this ) );
        }
    }

    private boolean brokerSaysIAmMaster()
    {
        // TODO Auto-generated method stub
        return false;
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
        shutdownIfNecessary();
    }

    private void shutdownIfNecessary()
    {
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
