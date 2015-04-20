/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.ComException;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.test.CleanupRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.ha.com.master.SlavePriorities.givenOrder;
import static org.neo4j.kernel.ha.com.master.SlavePriorities.roundRobin;

public class TestMasterCommittingAtSlave
{
    private static final int MasterServerId = 0;

    private Iterable<Slave> slaves;
    private FakeStringLogger log;

    @Test
    public void commitSuccessfullyToTheFirstOne() throws Exception
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder() );
        propagator.committed( 2, MasterServerId );
        assertCalls( (FakeSlave) slaves.iterator().next(), 2l );
        assertNoFailureLogs();
    }

    @Test
    public void commitACoupleOfTransactionsSuccessfully() throws Exception
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder() );
        propagator.committed( 2, MasterServerId );
        propagator.committed( 3, MasterServerId );
        propagator.committed( 4, MasterServerId );
        assertCalls( (FakeSlave) slaves.iterator().next(), 2, 3, 4 );
        assertNoFailureLogs();
    }

    @Test
    public void commitFailureAtFirstOneShouldMoveOnToNext() throws Exception
    {
        TransactionPropagator propagator = newPropagator( 3, 1, givenOrder(), true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next() );
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        assertNoFailureLogs();
    }

    @Test
    public void commitSuccessfullyAtThreeSlaves() throws Exception
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

        assertNoFailureLogs();
    }

    @Test
    public void commitSuccessfullyOnSomeOfThreeSlaves() throws Exception
    {
        TransactionPropagator propagator = newPropagator( 5, 3, givenOrder(), false, true, true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        slaveIt.next();
        slaveIt.next();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        assertNoFailureLogs();
    }

    @Test
    public void roundRobinSingleSlave() throws Exception
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
        assertNoFailureLogs();
    }

    @Test
    public void roundRobinSomeFailing() throws Exception
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
        assertNoFailureLogs();
    }

    @Test
    public void notEnoughSlavesSuccessful() throws Exception
    {
        TransactionPropagator propagator = newPropagator( 3, 2, givenOrder(), true, true );
        propagator.committed( 2, MasterServerId );
        Iterator<Slave> slaveIt = slaves.iterator();
        slaveIt.next();
        slaveIt.next();
        assertCalls( (FakeSlave) slaveIt.next(), 2 );
        assertFailureLogs();
    }

    @Test
    public void testFixedPriorityStrategy()
    {
        int[] serverIds = new int[]{55, 101, 66};
        SlavePriority fixed = SlavePriorities.fixed();
        ArrayList<Slave> slaves = new ArrayList<Slave>( 3 );
        slaves.add( new FakeSlave( false, serverIds[0] ) );
        slaves.add( new FakeSlave( false, serverIds[1] ) );
        slaves.add( new FakeSlave( false, serverIds[2] ) );
        Iterator<Slave> sortedSlaves = fixed.prioritize( slaves ).iterator();
        assertEquals( serverIds[1], sortedSlaves.next().getServerId() );
        assertEquals( serverIds[2], sortedSlaves.next().getServerId() );
        assertEquals( serverIds[0], sortedSlaves.next().getServerId() );
        assertTrue( !sortedSlaves.hasNext() );
    }

    private void assertNoFailureLogs()
    {
        assertFalse( "Errors:" + log.errors.toString(), log.unexpectedExceptionLogged );
    }

    private void assertFailureLogs()
    {
        assertTrue( log.unexpectedExceptionLogged );
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
                                                 boolean... failingSlaves ) throws Exception
    {
        slaves = instantiateSlaves( slaveCount, failingSlaves );

        log = new FakeStringLogger();
        Config config = new Config( MapUtil.stringMap(
                HaSettings.tx_push_factor.name(), "" + replication, ClusterSettings.server_id.name(), "" + MasterServerId ) );
        Neo4jJobScheduler scheduler = cleanup.add( new Neo4jJobScheduler() );
        TransactionPropagator result = new TransactionPropagator( TransactionPropagator.from( config, slavePriority ),
                log, new Slaves()
        {
            @Override
            public Iterable<Slave> getSlaves()
            {
                return slaves;
            }
        }, new CommitPusher( scheduler ) );
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
            throw Exceptions.launderedException( e );
        }
        return result;
    }

    private Iterable<Slave> instantiateSlaves( int count, boolean[] failingSlaves )
    {
        List<Slave> slaves = new ArrayList<Slave>();
        for ( int i = 0; i < count; i++ )
        {
            slaves.add( new FakeSlave( i < failingSlaves.length && failingSlaves[i], i + MasterServerId + 1 ) );
        }
        return slaves;
    }

    private static final FileSystemAbstraction FS = new DefaultFileSystemAbstraction();
    public final @Rule CleanupRule cleanup = new CleanupRule();

    private static class FakeSlave implements Slave
    {
        private volatile Queue<Long> calledWithTxId = new LinkedList<Long>();
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
            return new TransactionStreamResponse<>( null, new StoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
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

    private static class FakeStringLogger extends StringLogger
    {
        private volatile boolean unexpectedExceptionLogged;
        private final StringBuilder errors = new StringBuilder();

        @Override
        public void logLongMessage( String msg, Visitor<LineLogger, RuntimeException> source, boolean flush )
        {
            addError( msg );
        }

        private void addError( String msg )
        {
            if ( !msg.contains( "communication" ) )
            {
                unexpectedExceptionLogged = true;
            }
            errors.append( errors.length() > 0 ? "," : "" ).append( msg );
        }

        @Override
        public void logMessage( String msg, boolean flush )
        {
            addError( msg );
        }

        @Override
        public void logMessage( String msg, LogMarker marker )
        {
            addError( msg );
        }

        @Override
        public void logMessage( String msg, Throwable cause, boolean flush )
        {
            addError( msg );
        }

        @Override
        public void addRotationListener( Runnable listener )
        {
        }

        @Override
        public void flush()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        protected void logLine( String line )
        {
            addError( line );
        }
    }
}
