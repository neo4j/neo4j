package org.neo4j.kernel.impl.ha;

public final class SlaveContext
{
    private final int slaveId;
    private final long lastAppliedTransaction;

    public SlaveContext( int slaveId, long lastAppliedTransaction )
    {
        this.slaveId = slaveId;
        this.lastAppliedTransaction = lastAppliedTransaction;
    }

    public int slaveId()
    {
        return slaveId;
    }

    public long latestAppliedTransaction()
    {
        return lastAppliedTransaction;
    }
}
