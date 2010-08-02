package org.neo4j.kernel;

import java.io.Serializable;
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
        protected Node doOperation()
        {
            return localGraph.createNode();
        }
    };
    
    private final MasterFailureReactor<Node> getReferenceNodeMethod =
        new MasterFailureReactor<Node>( this )
    {
        @Override
        protected Node doOperation()
        {
            return localGraph.getReferenceNode();
        }
    };
    
    private final String storeDir;
    private final Map<String, String> config;
    private final Broker broker;
    private EmbeddedGraphDbImpl localGraph;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.broker = instantiateBroker();
        reevaluateMyself();
    }

    private Broker instantiateBroker()
    {
        String cls = config.get( "ha_broker" );
        cls = cls != null ? cls : "some.default.Broker";
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

    protected void reevaluateMyself()
    {
        shutdownIfNecessary();
        if ( brokerSaysIAmMaster() )
        {
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    LockManagerFactory.DEFAULT, IdGeneratorFactory.DEFAULT );
        }
        else
        {
            ResponseReceiver receiver = new ResponseReceiver();
            this.localGraph = new EmbeddedGraphDbImpl( storeDir, config, this,
                    new SlaveLockManager.SlaveLockManagerFactory( broker, receiver ),
                    new SlaveIdGenerator.SlaveIdGeneratorFactory( broker, receiver ) );
        }
    }

    private boolean brokerSaysIAmMaster()
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
        return createNodeMethod.execute();
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
        return getReferenceNodeMethod.execute();
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
