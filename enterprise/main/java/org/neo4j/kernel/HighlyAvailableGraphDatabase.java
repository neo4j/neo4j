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
import org.neo4j.kernel.ha.PulledOutHooksFromEmbeddedGraphDb;

public class HighlyAvailableGraphDatabase implements GraphDatabaseService
{
    private final PulledOutHooksFromEmbeddedGraphDb hooks;
    private final EmbeddedGraphDbImpl localGraph;

    public HighlyAvailableGraphDatabase()
    {
        this.hooks = null;
        this.localGraph = null;
    }

    public Transaction beginTx()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node createNode()
    {
        // TODO Auto-generated method stub
        return null;
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

}
