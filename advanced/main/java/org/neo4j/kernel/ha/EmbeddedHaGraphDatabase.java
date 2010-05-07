package org.neo4j.kernel.ha;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.ha.zookeeper.ZooClient;

public class EmbeddedHaGraphDatabase implements GraphDatabaseService
{
    private final ZooClient zooKeeperClient;
    
    // Switch this when a slave becomes master or master becomes slave
    private MasterOrSlaveEmbeddedGraphDb graphDbImpl;
    
    public EmbeddedHaGraphDatabase( String storeDir, int machineId,
            String zooKeeperServiceDefinition, Map<String, String> configuration )
    {
        // Connect to zoo keeper
        // Get the master, if I am the master instantiate a MasterEmbeddedGraphDbImpl, else a Slave...Impl
        // TODO (later) register event listeners that will get events (kerel panics or something)
        //               so that it can then ask zoo keeper what has happened and again instantiate
        //               master/slave depending on if I am the master.
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
