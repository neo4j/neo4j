package org.neo4j.kernel.impl.transaction.log;

import org.neo4j.kernel.impl.store.counts.CountsSnapshot;

public class LogRecoveryInfo
{
    LogPosition logPosition;
    CountsSnapshot countsSnapshot;


    public LogRecoveryInfo( LogPosition logPosition, CountsSnapshot countsSnapshot )
    {
        this.logPosition = logPosition;
        this.countsSnapshot = countsSnapshot;
    }

    public LogPosition getLogPosition()
    {
        return logPosition;
    }

    public CountsSnapshot getCountsSnapshot()
    {
        return countsSnapshot;
    }

}
