/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.ComException;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcher;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;
import static org.neo4j.kernel.ha.com.master.SlavePriorities.givenOrder;
import static org.neo4j.kernel.ha.com.master.SlavePriorities.roundRobin;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;

public class TestMasterCommittingAtSlave
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    private static final int MasterServerId = 0;

    private Iterable<Slave> slaves;
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private LogMatcher communicationLogMessage = new LogMatcher( any( String.class ), is( ERROR ),
            containsString( "communication" ), any( Object[].class ), any( Throwable.class ) );

    @Test
    public void commitSuccessfullyToTheFirstOne()
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder() );
        propagator.committed( 2, MasterServerId );
        assertCalls( (FakeSlave) slaves.iterator().next(), 2L );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void commitACoupleOfTransactionsSuccessfully()
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder() );
        propagator.committed( 2, MasterServerId );
        propagator.committed( 3, MasterServerId );
        propagator.committed( 4, MasterServerId );
        assertCalls( (FakeSlave) slaves.iterator().next(), 2, 3, 4 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void commitFailureAtFirstOneShouldMoveOnToNext()
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder(), true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next() );
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void commitSuccessfullyAtThreeSlaves()
    {
        TransactionPropagator propagator = newPropagator( 5, 3, givenOrder() );
        propagator.committed( 2, MasterServerId );
        propagator.committed( 3, 1 );
        propagator.committed( 4, 2 );

        Iterator<Slave> slaveIt = slaves.iterator();

        assertCalls( (FakeSlave) slaveIt.next(), 2, 4 );
        assertCalls( (FakeSlave) slaveIt.next(), 2, 3 );
        assertCalls( (FakeSlave) slaveIt.next(), 2, 3, 4 );
        assertCalls( (FakeSlave) slaveIt.next() );
        assertCalls( (FakeSlave) slaveIt.next() );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void commitSuccessfullyOnSomeOfThreeSlaves()
    {
        TransactionPropagator propagator = newPropagator( 5, 3, givenOrder(), false, true, true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        slaveIt.next();
        slaveIt.next();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void roundRobinSingleSlave()
    {
        TransactionPropagator propagator = newPropagator( 3, 1, roundRobin() );
        for ( long tx = 2; tx <= 6; tx++ )
        {
            propagator.committed( tx, MasterServerId );
        }
        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next(), 2, 5 );
        assertCalls( (FakeSlave) slaveIt.next(), 3, 6 );
        assertCalls( (FakeSlave) slaveIt.next(), 4 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void roundRobinSomeFailing()
    {
        TransactionPropagator propagator = newPropagator( 4, 2, roundRobin(), false, true );
        for ( long tx = 2; tx <= 6; tx++ )
        {
            propagator.committed( tx, MasterServerId );
        }

        /* SLAVE |    TX
        *   0   | 2     5 6
        * F 1   |
        *   2   | 2 3 4   6
        *   3   |   3 4 5
        */

        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next(), 2, 5, 6 );
        slaveIt.next();
        assertCalls( (FakeSlave) slaveIt.next(), 2, 3, 4, 6 );
        assertCalls( (FakeSlave) slaveIt.next(), 3, 4, 5 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void notEnoughSlavesSuccessful()
    {
        TransactionPropagator propagator = newPropagator( 3, 2, givenOrder(), true, true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        slaveIt.next();
        slaveIt.next();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        logProvider.assertNone( communicationLogMessage );
    }

    @Test
    public void testFixedPriorityStrategy()
    {
        int[] serverIds = new int[]{55, 101, 66};
        SlavePriority fixed = SlavePriorities.fixedDescending();
        ArrayList<Slave> slaves = new ArrayList<>( 3 );
        slaves.add( new FakeSlave( false, serverIds[0] ) );
        slaves.add( new FakeSlave( false, serverIds[1] ) );
        slaves.add( new FakeSlave( false, serverIds[2] ) );
        Iterator<Slave> sortedSlaves = fixed.prioritize( slaves ).iterator();
        assertEquals( serverIds[1], sortedSlaves.next().getServerId() );
        assertEquals( serverIds[2], sortedSlaves.next().getServerId() );
        assertEquals( serverIds[0], sortedSlaves.next().getServerId() );
        assertTrue( !sortedSlaves.hasNext() );
    }

    private void assertCalls( FakeSlave slave, long... txs )
    {
        for ( long tx : txs )
        {
            Long slaveTx = slave.popCalledTx();
            assertNotNull( slaveTx );
            assertEquals( (Long) tx, slaveTx );
        }
        assertFalse( slave.moreTxs() );
    }

    private TransactionPropagator newPropagator( int slaveCount, int replication, SlavePriority slavePriority,
                                                 boolean... failingSlaves )
    {
        slaves = instantiateSlaves( slaveCount, failingSlaves );

        Config config = Config.defaults( MapUtil.stringMap(
                HaSettings.tx_push_factor.name(), "" + replication, ClusterSettings.server_id.name(), "" + MasterServerId ) );
        CentralJobScheduler scheduler = cleanup.add( new CentralJobScheduler() );
        TransactionPropagator result = new TransactionPropagator( TransactionPropagator.from( config, slavePriority ),
                NullLog.getInstance(), () -> slaves, new CommitPusher( scheduler ) );
        // Life
        try
        {
            scheduler.init();
            scheduler.start();

            result.init();
            result.start();
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
        return result;
    }

    private Iterable<Slave> instantiateSlaves( int count, boolean[] failingSlaves )
    {
        List<Slave> slaves = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            slaves.add( new FakeSlave( i < failingSlaves.length && failingSlaves[i], i + MasterServerId + 1 ) );
        }
        return slaves;
    }

    private static class FakeSlave implements Slave
    {
        private volatile Queue<Long> calledWithTxId = new LinkedList<>();
        private final boolean failing;
        private final int serverId;

        FakeSlave( boolean failing, int serverId )
        {
            this.failing = failing;
            this.serverId = serverId;
        }

        @Override
        public Response<Void> pullUpdates( long txId )
        {
            if ( failing )
            {
                throw new ComException( "Told to fail" );
            }

            calledWithTxId.add( txId );
            return new TransactionStreamResponse<>( null, newStoreIdForCurrentVersion(), TransactionStream.EMPTY,
                    ResourceReleaser.NO_OP );
        }

        Long popCalledTx()
        {
            return calledWithTxId.poll();
        }

        boolean moreTxs()
        {
            return !calledWithTxId.isEmpty();
        }

        @Override
        public int getServerId()
        {
            return serverId;
        }

        @Override
        public String toString()
        {
            return "FakeSlave[" + serverId + "]";
        }
    }
}
