package org.neo4j.server.ext.visualization.graph;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;

import java.io.Serializable;
import java.util.Map;

/**
 * Enhances a GraphDatabaseService with convenience methods for visualizing.
 *
 */
public class VisualGraphDatabase implements GraphDatabaseService
{
   private GraphDatabaseService graphDb;

    public VisualGraphDatabase(GraphDatabaseService graphDb)
    {
        this.graphDb = graphDb;
    }

    public Transaction beginTx()
    {
        return graphDb.beginTx();
    }

    public Node createNode()
    {
        return graphDb.createNode();
    }

    public Node getNodeById( long l )
    {
        return graphDb.getNodeById( l );
    }

    public Relationship getRelationshipById( long l )
    {
        return graphDb.getRelationshipById( l );
    }

    public Node getReferenceNode()
    {
        return graphDb.getReferenceNode();
    }

    public Iterable<Node> getAllNodes()
    {
        return graphDb.getAllNodes();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return graphDb.getRelationshipTypes();
    }

    public void shutdown()
    {
        graphDb.shutdown();
    }

    public IndexManager index()
    {
        return graphDb.index();
    }

    //
    // GraphDatabaseService no-ops
    // intentionally not implemented
    //
    public boolean enableRemoteShell()
    {
        return false;
    }

    public boolean enableRemoteShell( Map<String, Serializable> stringSerializableMap )
    {
        return false;
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> tTransactionEventHandler )
    {
        return null;
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> tTransactionEventHandler )
    {
        return null;
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler kernelEventHandler )
    {
        return null;
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler kernelEventHandler )
    {
        return null;
    }
}
