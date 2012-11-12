/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.ha.SlavePriorities.givenOrder;
import static org.neo4j.kernel.ha.SlavePriorities.roundRobin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.junit.Test;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class TestMasterCommittingAtSlave
{
    FakeSlave[] slaves;
    XaDataSource dataSource;
    Broker broker;
    FakeStringLogger log;
    
    @Test
    public void commitSuccessfullyToTheFirstOne() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 3, 1, givenOrder() );
        generator.committed( dataSource, 0, 2, null );
        assertCalls( slaves[0], 2 );
        assertNoFailureLogs();
    }
    
    @Test
    public void commitACoupleOfTransactionsSuccessfully() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 3, 1, givenOrder() );
        generator.committed( dataSource, 0, 2, null );
        generator.committed( dataSource, 0, 3, null );
        generator.committed( dataSource, 0, 4, null );
        assertCalls( slaves[0], 2, 3, 4 );
        assertNoFailureLogs();
    }
    
    @Test
    public void commitFailureAtFirstOneShouldMoveOnToNext() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 3, 1, givenOrder(), true );
        generator.committed( dataSource, 0, 2, null );
        assertCalls( slaves[0] );
        assertCalls( slaves[1], 2 );
        assertNoFailureLogs();
    }
    
    @Test
    public void commitSuccessfullyAtThreeSlaves() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 5, 3, givenOrder() );
        generator.committed( dataSource, 0, 2, null );
        generator.committed( dataSource, 0, 3, 1 );
        generator.committed( dataSource, 0, 4, 3 );
        
        assertCalls( slaves[0], 2, 3, 4 );
        assertCalls( slaves[1], 2, 4 );
        assertCalls( slaves[2], 2, 3 );
        assertCalls( slaves[3] );
        assertCalls( slaves[4] );
        
        assertNoFailureLogs();
    }
    
    @Test
    public void commitSuccessfullyOnSomeOfThreeSlaves() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 5, 3, givenOrder(), false, true, true );
        generator.committed( dataSource, 0, 2, null );
        assertCalls( slaves[0], 2 );
        assertCalls( slaves[3], 2 );
        assertCalls( slaves[4], 2 );
        assertNoFailureLogs();
    }
    
    @Test
    public void roundRobinSingleSlave() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 3, 1, roundRobin() );
        for ( long tx = 2; tx <= 6; tx++ )
            generator.committed( dataSource, 0, tx, null );
        
        assertCalls( slaves[0], 2, 5 );
        assertCalls( slaves[1], 3, 6 );
        assertCalls( slaves[2], 4 );
        assertNoFailureLogs();
    }
    
    @Test
    public void roundRobinSomeFailing() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 4, 2, roundRobin(), false, true );
        for ( long tx = 2; tx <= 6; tx++ )
            generator.committed( dataSource, 0, tx, null );
        
        /* SLAVE |    TX
         *   0   | 2     5 6
         * F 1   |
         *   2   | 2 3 4   6
         *   3   |   3 4 5
         */
        
        assertCalls( slaves[0], 2, 5, 6 );
        assertCalls( slaves[2], 2, 3, 4, 6 );
        assertCalls( slaves[3], 3, 4, 5 );
        assertNoFailureLogs();
    }
    
    @Test
    public void notEnoughSlavesSuccessful() throws Exception
    {
        MasterTxIdGenerator generator = newGenerator( 3, 2, givenOrder(), true, true );
        generator.committed( dataSource, 0, 2, null );
        assertCalls( slaves[2], 2 );
        assertFailureLogs();
    }
    
    @Test
    public void testFixedPriorityStrategy()
    {
        SlavePriority fixed = SlavePriorities.fixed();
        Slave[] slaves = new Slave[3];
        slaves[0] = new FakeSlave( false, 55 );
        slaves[1] = new FakeSlave( false, 101 );
        slaves[2] = new FakeSlave( false, 66 );
        Iterator<Slave> sortedSlaves = fixed.prioritize( slaves );
        assertEquals( slaves[1], sortedSlaves.next() );
        assertEquals( slaves[2], sortedSlaves.next() );
        assertEquals( slaves[0], sortedSlaves.next() );
        assertTrue( !sortedSlaves.hasNext() );
    }
    
    private void assertNoFailureLogs()
    {
        assertFalse( "Errors:" + log.errors.toString(), log.anyMessageLogged );
    }
    
    private void assertFailureLogs()
    {
        assertTrue( log.anyMessageLogged );
    }
    
    private void assertCalls( FakeSlave slave, long... txs )
    {
        for ( long tx : txs )
        {
            Long slaveTx = slave.popCalledTx();
            assertNotNull( slaveTx );
            assertEquals( (Long)tx, slaveTx );
        }
        assertFalse( slave.moreTxs() );
    }

    private MasterTxIdGenerator newGenerator( int slaveCount, int replication, SlavePriority priority,
            boolean... failingSlaves ) throws Exception
    {
        slaves = instantiateSlaves( slaveCount, failingSlaves );
        dataSource = new FakeDataSource();
        
        broker = new FakeBroker( slaves );
        log = new FakeStringLogger();
        MasterTxIdGenerator result = new MasterTxIdGenerator( broker, replication, priority, log );
        // Life
        try
        {
            result.init();
            result.start();
        }
        catch ( Throwable e )
        {
            throw Exceptions.launderedException( e );
        }
        return result;
    }
    
    private FakeSlave[] instantiateSlaves( int count, boolean[] failingSlaves )
    {
        Collection<FakeSlave> slaves = new ArrayList<FakeSlave>();
        for ( int i = 0; i < count; i++ )
            slaves.add( new FakeSlave( i < failingSlaves.length ? failingSlaves[i] : false, i ) );
        return slaves.toArray( new FakeSlave[slaves.size()] );
    }
    
    private static class FakeDataSource extends XaDataSource
    {
        private static final byte[] BRANCH = new byte[] { 0, 1, 2 };
        private static final String NAME = "fake";
        
        private final String dir;

        FakeDataSource()
        {
            super( BRANCH, NAME );
            this.dir = TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
        }
        
        @Override
        public XaConnection getXaConnection()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
        
        @Override
        public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
        {
            return LogExtractor.from( dir, startTxId );
        }
    }

    private static class FakeBroker extends AbstractBroker
    {
        private final Slave[] slaves;

        FakeBroker( Slave[] slaves )
        {
            super( null );
            this.slaves = slaves;
        }
        
        @Override
        public int getMyMachineId()
        {
            return 0;
        }
        
        @Override
        public Slave[] getSlaves()
        {
            return slaves;
        }

        @Override
        public Pair<Master, Machine> getMaster()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Pair<Master, Machine> getMasterReally( boolean allowChange )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean iAmMaster()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object instantiateMasterServer( GraphDatabaseAPI graphDb )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object instantiateSlaveServer( GraphDatabaseAPI graphDb, SlaveDatabaseOperations ops )
        {
            throw new UnsupportedOperationException();
        }
    }
    
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
        public Response<Void> pullUpdates( String resource, long txId )
        {
            if ( failing )
                throw new RuntimeException( "Told to fail" );
            
            calledWithTxId.add( txId );
            return new Response<Void>( null, new StoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
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
        private volatile boolean anyMessageLogged;
        private StringBuilder errors = new StringBuilder();
        
        @Override
        public void logLongMessage( String msg, Visitor<LineLogger> source, boolean flush )
        {
            addError( msg );
        }

        private void addError( String msg )
        {
            anyMessageLogged = true;
            errors.append( errors.length() > 0 ? "," : "" ).append( msg );
        }

        @Override
        public void logMessage( String msg, boolean flush )
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
