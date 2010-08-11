package org.neo4j.kernel.impl.ha;

import java.util.Map;

public final class SlaveContext
{
    private final int machineId;
    private final Map<String, Long> lastAppliedTransactions;

    public SlaveContext( int machineId, Map<String, Long> lastAppliedTransactions )
    {
        this.machineId = machineId;
        this.lastAppliedTransactions = lastAppliedTransactions;
    }

    public int machineId()
    {
        return machineId;
    }

    public Map<String, Long> lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }
}
