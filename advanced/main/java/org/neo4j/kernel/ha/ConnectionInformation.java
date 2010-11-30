package org.neo4j.kernel.ha;

import java.net.MalformedURLException;

import javax.management.remote.JMXServiceURL;

import org.neo4j.kernel.ha.zookeeper.Machine;

public class ConnectionInformation
{
    private final boolean master;
    private final int machineId;
    private JMXServiceURL jmxURL;
    private String instanceId;
    private final long txId;

    public ConnectionInformation( Machine machine, boolean master )
    {
        this.master = master;
        this.machineId = machine.getMachineId();
        this.txId = machine.getLastCommittedTxId();
    }

    public void setJMXConnectionData( String url, String instanceId )
    {
        this.instanceId = instanceId;
        try
        {
            this.jmxURL = new JMXServiceURL( url );
        }
        catch ( MalformedURLException e )
        {
            this.jmxURL = null;
        }
    }

    public JMXServiceURL getJMXServiceURL()
    {
        return jmxURL;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return master;
    }

    public long getLastCommitedTransactionId()
    {
        return txId;
    }
}
