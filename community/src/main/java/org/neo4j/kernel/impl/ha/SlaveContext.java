package org.neo4j.kernel.impl.ha;

import org.neo4j.helpers.Pair;

public final class SlaveContext
{
    private final int machineId;
    private final Pair<String, Long>[] lastAppliedTransactions;

    public SlaveContext( int machineId, Pair<String, Long>[] lastAppliedTransactions )
    {
        this.machineId = machineId;
        this.lastAppliedTransactions = lastAppliedTransactions;
    }

    public int machineId()
    {
        return machineId;
    }

    public Pair<String, Long>[] lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }
}
