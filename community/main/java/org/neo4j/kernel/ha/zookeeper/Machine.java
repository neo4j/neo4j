package org.neo4j.kernel.ha.zookeeper;

import org.neo4j.helpers.Pair;

public class Machine
{
    public static final Machine NO_MACHINE = new Machine( -1, -1, 1, null );
    
    private final int machineId;
    private final int sequenceId;
    private final long lastCommittedTxId;
    private final Pair<String, Integer> server;

    public Machine( int machineId, int sequenceId, long lastCommittedTxId, String server )
    {
        this.machineId = machineId;
        this.sequenceId = sequenceId;
        this.lastCommittedTxId = lastCommittedTxId;
        this.server = server != null ? splitIpAndPort( server ) : null;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public long getLastCommittedTxId()
    {
        return lastCommittedTxId;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    public Pair<String, Integer> getServer()
    {
        return server;
    }
    
    @Override
    public String toString()
    {
        return "MachineInfo[ID:" + machineId + ", sequence:" + sequenceId +
                ", last committed tx id:" + lastCommittedTxId + ", server:" + server + "]";
    }
    
    @Override
    public boolean equals( Object obj )
    {
        return (obj instanceof Machine) && ((Machine) obj).machineId == machineId;
    }
    
    @Override
    public int hashCode()
    {
        return machineId*19;
    }

    public static Pair<String, Integer> splitIpAndPort( String server )
    {
        int pos = server.indexOf( ":" );
        return new Pair<String, Integer>( server.substring( 0, pos ),
                Integer.parseInt( server.substring( pos + 1 ) ) );
    }
}
