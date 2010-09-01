package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;

public class ZooKeeperLastCommittedTxIdSetter implements LastCommittedTxIdSetter
{
    private final Broker broker;

    public ZooKeeperLastCommittedTxIdSetter( Broker broker )
    {
        this.broker = broker;
    }

    public void setLastCommittedTxId( long txId )
    {
        broker.setLastCommittedTxId( txId );
    }
}
