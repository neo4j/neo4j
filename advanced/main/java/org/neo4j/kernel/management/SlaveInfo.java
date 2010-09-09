package org.neo4j.kernel.management;

import java.io.Serializable;
import java.util.Map;

public final class SlaveInfo implements Serializable
{
    private static final long serialVersionUID = 1L;

    public static final class SlaveTransaction implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private final int eventId;
        private final Map<String, Long> lastTransactions;

        public SlaveTransaction( int eventId, Map<String, Long> lastTransactions )
        {
            this.eventId = eventId;
            this.lastTransactions = lastTransactions;
        }

        public int getEventIdentifier()
        {
            return eventId;
        }

        public long getLastTransaction( String resource )
        {
            return lastTransactions.get( resource );
        }
    }

    private final Integer machineId;
    private final SlaveTransaction[] txInfo;

    public SlaveInfo( Integer machineId, SlaveTransaction... txInfo )
    {
        this.machineId = machineId;
        this.txInfo = txInfo;
    }

    public Integer getMachineId()
    {
        return machineId;
    }

    public SlaveTransaction[] getTxInfo()
    {
        return txInfo;
    }
}
