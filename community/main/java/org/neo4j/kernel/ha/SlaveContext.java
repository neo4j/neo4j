package org.neo4j.kernel.ha;

import java.util.Arrays;

import org.neo4j.helpers.Pair;

public final class SlaveContext
{
    private final int machineId;
    private final Pair<String, Long>[] lastAppliedTransactions;
    private final int eventIdentifier;
    private final int hashCode;

    public SlaveContext( int machineId, int eventIdentifier,
            Pair<String, Long>[] lastAppliedTransactions )
    {
        this.machineId = machineId;
        this.eventIdentifier = eventIdentifier;
        this.lastAppliedTransactions = lastAppliedTransactions;
        this.hashCode = 3*eventIdentifier*machineId;
    }

    public int machineId()
    {
        return machineId;
    }

    public Pair<String, Long>[] lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }
    
    public int getEventIdentifier()
    {
        return eventIdentifier;
    }

    @Override
    public String toString()
    {
        return "SlaveContext[ID:" + machineId + ", eventIdentifier:" + eventIdentifier + ", " +
                Arrays.asList( lastAppliedTransactions ) + "]";
    }
    
    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof SlaveContext ) )
        {
            return false;
        }
        SlaveContext o = (SlaveContext) obj;
        return o.eventIdentifier == eventIdentifier && o.machineId == machineId;
    }
    
    @Override
    public int hashCode()
    {
        return this.hashCode;
    }
}
