package org.neo4j.kernel;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.ha.SlaveIdGenerator;
import org.neo4j.kernel.ha.SlaveLockManager;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTopLevelTransactionFactory;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.ResponseReceiver;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService
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

    protected void reevaluateMyself()
    {
        shutdownIfNecessary();
        if ( brokerSaysIAmMaster() )
        {
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    LockManagerFactory.DEFAULT, IdGeneratorFactory.DEFAULT,
                    DefaultRelationshipTypeCreator.INSTANCE, TopLevelTransactionFactory.DEFAULT );
        }
        else
        {
            ResponseReceiver receiver = new ResponseReceiver();
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    new SlaveLockManager.SlaveLockManagerFactory( broker, receiver ),
                    new SlaveIdGenerator.SlaveIdGeneratorFactory( broker, receiver ),
                    new SlaveRelationshipTypeCreator( broker, receiver ),
                    new SlaveTopLevelTransactionFactory( broker, receiver ) );
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
}
