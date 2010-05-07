package org.neo4j.kernel.ha;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class SlaveEmbeddedGraphDbImpl implements MasterOrSlaveEmbeddedGraphDb
{
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

    public boolean enableRemoteShell( Map<String, Serializable> arg0 )
    {
        // TODO Auto-generated method stub
        return false;
    }

    public Iterable<Node> getAllNodes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node getNodeById( long arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node getReferenceNode()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship getRelationshipById( long arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void shutdown()
    {
        // TODO Auto-generated method stub

    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> arg0 )
    {
        // TODO Auto-generated method stub
        return null;
    }

}
