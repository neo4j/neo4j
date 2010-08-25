package org.neo4j.kernel.ha.zookeeper;

import org.neo4j.helpers.Pair;

public class Machine
{
    private final int machineId;
    private final int sequenceId;
    private final long latestTxId;
    private final Pair<String, Integer> server;

    Machine( int machineId, int sequenceId, long lastestTxId, String server )
    {
        this.machineId = machineId;
        this.sequenceId = sequenceId;
        this.latestTxId = lastestTxId;
        this.server = server != null ? splitIpAndPort( server ) : null;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public long getLatestTxId()
    {
        return latestTxId;
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
                ", latest tx id:" + latestTxId + ", server:" + server + "]";
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
