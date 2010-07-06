package org.neo4j.kernel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.ha.SlaveIdGenerator;
import org.neo4j.kernel.ha.SlaveLockManager;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.transaction.TxManager;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService
{
    private final MasterFailureReactor<Node> createNodeMethod =
        new MasterFailureReactor<Node>( this )
    {
        @Override
        public Node doOperation()
        {
            return localGraph.createNode();
        }
    };
    
    private final Broker broker;
    private EmbeddedGraphDbImpl localGraph;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this.broker = instantiateBroker( config );
        evaluateMyself();
    }

    private Broker instantiateBroker( Map<String, String> config )
    {
        String cls = config.get( "ha_broker" );
        try
        {
            return Class.forName( cls ).asSubclass( Broker.class ).getConstructor(
                    Map.class ).newInstance( config );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void evaluateMyself()
    {
        if ( iAmMaster() )
        {
            
        }
        else
        {
            ResponseReceiver receiver = null;
            this.localGraph = new EmbeddedGraphDbImpl( "storeDir", new HashMap<String, String>(),
                    this, new SlaveLockManager.SlaveLockManagerFactory( broker, receiver ),
                    new SlaveIdGenerator.SlaveIdGeneratorFactory( broker, receiver ) );
        }
    }

    private boolean iAmMaster()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public Transaction beginTx()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node createNode()
    {
        return createNodeMethod.doOperation();
    }

    public boolean enableRemoteShell()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean enableRemoteShell( Map<String, Serializable> initialProperties )
    {
        // TODO Auto-generated method stub
        return false;
    }

    public Iterable<Node> getAllNodes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node getNodeById( long id )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node getReferenceNode()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship getRelationshipById( long id )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void shutdown()
    {
        // TODO Auto-generated method stub

    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    private class SlaveTransaction extends TopLevelTransaction
    {
        SlaveTransaction( TransactionManager tm )
        {
            super( tm );
        }
        
        public void finish()
        {
            boolean successfulFinish = false;
            try
            {
                int localTxId = ((TxManager) getTransactionManager()).getEventIdentifier(); 
                if ( isMarkedAsSuccessful() )
                {
                    broker.getMaster().commitTransaction( broker.getSlaveContext(),
                            localTxId, transactionAsStream() );
                }
                else
                {
                    broker.getMaster().rollbackTransaction( broker.getSlaveContext(),
                            localTxId );
                }
                successfulFinish = true;
            }
            finally
            {
                try
                {
                    if ( !successfulFinish )
                    {
                        failure();
                    }
                }
                finally
                {
                    super.finish();
                }
            }
        }

        private TransactionStream transactionAsStream()
        {
            throw new UnsupportedOperationException();
        }
    }
}
