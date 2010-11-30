package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;

import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.Pair;

public class InstanceInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    private final JMXServiceURL address;
    private final String instanceId;
    private final int machineId;
    private final boolean master;
    private final long lastTxId;

    @ConstructorProperties( { "address", "instanceId", "machineId", "master",
            "lastCommittedTransactionId" } )
    public InstanceInfo( JMXServiceURL address, String instanceId, int machineId, boolean master,
            long lastTxId )
    {
        this.address = address;
        this.instanceId = instanceId;
        this.machineId = machineId;
        this.master = master;
        this.lastTxId = lastTxId;
    }

    public boolean isMaster()
    {
        return master;
    }

    public JMXServiceURL getAddress()
    {
        return address;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public long getLastCommittedTransactionId()
    {
        return lastTxId;
    }

    public Pair<Neo4jManager, HighAvailability> connect()
    {
        return connect( null, null );
    }

    public Pair<Neo4jManager, HighAvailability> connect( String username, String password )
    {
        if ( address == null )
        {
            throw new IllegalStateException( "The instance does not have a public JMX server." );
        }
        Neo4jManager manager = Neo4jManager.get( address, username, password, instanceId );
        return Pair.of( manager, manager.getBean( HighAvailability.class ) );
    }
}
