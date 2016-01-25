package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;

import org.neo4j.kernel.impl.store.counts.CountsSnapshot;

import static org.junit.Assert.assertEquals;

public class LogRecoveryInfoTest
{
    LogRecoveryInfo logRecoveryInfo = new LogRecoveryInfo( new LogPosition( 42, 0 ), new CountsSnapshot( 42 ) );

    @Test
    public void testGetLogPosition() throws Exception
    {
        assertEquals( 42, logRecoveryInfo.getLogPosition().getLogVersion() );
    }

    @Test
    public void testGetCountsSnapshot() throws Exception
    {
        CountsSnapshot testSnapshot = new CountsSnapshot( 42 );
        assertEquals( testSnapshot.getTxId(), logRecoveryInfo.getCountsSnapshot().getTxId() );
    }
}