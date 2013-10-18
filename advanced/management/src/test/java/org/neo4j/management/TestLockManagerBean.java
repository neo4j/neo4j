/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.management;

import java.lang.Thread.State;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.info.LockingTransaction;
import org.neo4j.kernel.info.ResourceType;
import org.neo4j.kernel.info.WaitingThread;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.*;

public class TestLockManagerBean
{
    private LockManager lockManager;

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    private GraphDatabaseAPI graphDb;

    @Before
    public void setup()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
        lockManager = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( LockManager.class );
    }

    @Test
    public void restingGraphHoldsNoLocks()
    {
        assertEquals( "unexpected lock count", 0, lockManager.getLocks().size() );
    }

    @Test
    public void modifiedNodeImpliesLock()
    {
        Node node;
        try(Transaction tx = graphDb.beginTx())
        {
            node = graphDb.createNode();
            tx.success();
        }

        try(Transaction ignore = graphDb.beginTx())
        {
            node.setProperty( "key", "value" );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 1, locks.size() );
            LockInfo lock = locks.get( 0 );
            assertNotNull( "null lock", lock );
            Collection<LockingTransaction> transactions = lock.getLockingTransactions();
            assertEquals( "unexpected transaction count", 1, transactions.size() );
            LockingTransaction txInfo = transactions.iterator().next();
            assertNotNull( "null transaction", txInfo );
            assertEquals( "read count", 0, txInfo.getReadCount() );
            
            /* Before property handling moved from Primitive into the Kernel API there were two
             * locks acquired for setting a property. One was about acquiring a write lock for that entity
             * before even accessing the Primitive. The other one was the normal write lock for a change
             * to an entity. The former guarded for a property data race, which is at the point of writing this
             * unknown if it exists after the move or not (which also made the change to only acquire
             * one lock again). */
            assertEquals( "write count should be 1", 1, txInfo.getWriteCount() );
            assertNotNull( "transaction", txInfo.getTransaction() );

            assertEquals( "read count", 0, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );

            assertEquals( "waiting thread count", 0, lock.getWaitingThreadsCount() );
        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.size() );
    }

    @Test
    public void explicitLocksAffectTheLockCount()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node root = graphDb.createNode();
            Lock first = tx.acquireReadLock( root );

            LockInfo lock = getSingleLock();
            assertEquals( "read count", 1, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );

            tx.acquireReadLock( root );
            lock = getSingleLock();
            assertEquals( "read count", 2, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );

            tx.acquireWriteLock( root );
            lock = getSingleLock();
            assertEquals( "read count", 2, lock.getReadCount() );
            assertEquals( "write count", 2, lock.getWriteCount() );

            first.release();
            lock = getSingleLock();
            assertEquals( "read count", 1, lock.getReadCount() );
            assertEquals( "write count", 2, lock.getWriteCount() );
        }
        finally
        {
            tx.finish();
        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.size() );
    }

    @Test
    public void canGetToContendedLocksOnly() throws Exception
    {
        Node root;
        try(Transaction tx = graphDb.beginTx())
        {
            root = graphDb.createNode();
            tx.success();
        }
        Transaction tx = graphDb.beginTx();
        try
        {
            graphDb.createNode();
            Lock lock = tx.acquireReadLock( root );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 2, locks.size() );
            for ( LockInfo l : locks )
            {
                switch ( l.getResourceType() )
                {
                    case NODE:
                        if ( l.getResourceId().equals( String.valueOf( root.getId() ) ) )
                        {
                            assertEquals( "read count", 1, l.getReadCount() );
                            assertEquals( "write count", 0, l.getWriteCount() );
                        }
                        else
                        {
                            assertEquals( "read count", 0, l.getReadCount() );
                            assertEquals( "write count", 1, l.getWriteCount() );
                        }
                        break;
                    default:
                        fail( "Unexpected locked resource type: " + l.getResourceType() );
                }
            }
            final CountDownLatch latch = new CountDownLatch( 1 );
            final Node finalRoot = root;
            Thread t = new Thread()
            {
                @Override
                public void run()
                {
                    Transaction tx = graphDb.beginTx();
                    try
                    {
                        finalRoot.setProperty( "block", "here" );
                    }
                    finally
                    {
                        tx.finish();
                    }
                    latch.countDown();
                }
            };
            t.start();
            awaitWaitingStateIn( t );

            locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 2, locks.size() );
            for ( LockInfo l : locks )
            {
                switch ( l.getResourceType() )
                {
                    case NODE:
                        if ( "0".equals( l.getResourceId() ) )
                        {
                            assertEquals( "read count", 1, l.getReadCount() );
                            assertEquals( "write count", 0, l.getWriteCount() );
                            List<WaitingThread> waiters = l.getWaitingThreads();
                            assertEquals( "unxpected number of waiting threads", 1, waiters.size() );
                            WaitingThread waiter = waiters.get( 0 );
                            assertNotNull( waiter );
                        }
                        else
                        {
                            assertEquals( "read count", 0, l.getReadCount() );
                            assertEquals( "write count", 1, l.getWriteCount() );
                        }
                        break;
                    default:
                        fail( "Unexpected locked resource type: " + l.getResourceType() );
                }
            }
            locks = lockManager.getContendedLocks( 0 );
            assertEquals( "unexpected lock count", 1, locks.size() );
            LockInfo l = locks.get( 0 );
            assertEquals( "resource type", ResourceType.NODE, l.getResourceType() );
            assertEquals( "resource id", "0", l.getResourceId() );
            assertEquals( "read count", 1, l.getReadCount() );
            assertEquals( "write count", 0, l.getWriteCount() );
            List<WaitingThread> waiters = l.getWaitingThreads();
            assertEquals( "unxpected number of waiting threads", 1, waiters.size() );
            WaitingThread waiter = waiters.get( 0 );
            assertNotNull( waiter );

            lock.release();
            latch.await();
        }
        finally
        {
            tx.finish();
        }
    }

    private void awaitWaitingStateIn( Thread t )
    {
        while ( t.getState() != State.WAITING )
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    private LockInfo getSingleLock()
    {
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 1, locks.size() );
        LockInfo lock = locks.get( 0 );
        assertNotNull( "null lock", lock );
        return lock;
    }
}
