package org.neo4j.kernel.impl.ha;

import java.util.Map;

public final class SlaveContext
{
    private final int slaveId;
    private final Map<String, Long> lastAppliedTransactions;

    public SlaveContext( int slaveId, Map<String, Long> lastAppliedTransactions )
    {
        this.slaveId = slaveId;
        this.lastAppliedTransactions = lastAppliedTransactions;
    }

    public int slaveId()
    {
        return slaveId;
    }

    public Map<String, Long> lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }
}
