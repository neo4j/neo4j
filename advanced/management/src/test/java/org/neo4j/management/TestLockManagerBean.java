/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.info.LockingTransaction;

public class TestLockManagerBean
{
    private LockManager lockManager;

    @Before
    public void setupLockManager()
    {
        lockManager = graphDb.getSingleManagementBean( LockManager.class );
    }

    @Test
    public void restingGraphHoldsNoLocks()
    {
        assertEquals( "unexpected lock count", 0, lockManager.getLocks().length );
    }

    @Test
    public void modifiedNodeImpliesLock()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            graphDb.getReferenceNode().setProperty( "key", "value" );

            LockInfo[] locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 1, locks.length );
            LockInfo lock = locks[0];
            assertNotNull( "null lock", lock );
            Collection<LockingTransaction> transactions = lock.getLockingTransactions();
            assertEquals( "unexpected transaction count", 1, transactions.size() );
            LockingTransaction txInfo = transactions.iterator().next();
            assertNotNull( "null transaction", txInfo );
            assertEquals( "read count", 0, txInfo.getReadCount() );
            assertEquals( "write count", 1, txInfo.getWriteCount() );
            assertNotNull( "transaction", txInfo.getTransaction() );

            assertEquals( "read count", 0, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );

            assertEquals( "waiting thread count", 0, lock.getWaitingThreadsCount() );
        }
        finally
        {
            tx.finish();
        }
        LockInfo[] locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.length );
    }
    
    @Test
    public void explicitLocksAffectTheLockCount()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node root = graphDb.getReferenceNode();
            Lock first = tx.acquireReadLock( root );
            
            LockInfo lock = getSingleLock();
            assertEquals( "read count", 1, lock.getReadCount() );
            assertEquals( "write count", 0, lock.getWriteCount() );
            
            tx.acquireReadLock( root );
            lock = getSingleLock();
            assertEquals( "read count", 2, lock.getReadCount() );
            assertEquals( "write count", 0, lock.getWriteCount() );
            
            tx.acquireWriteLock( root );
            lock = getSingleLock();
            assertEquals( "read count", 2, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );
            
            first.release();
            lock = getSingleLock();
            assertEquals( "read count", 1, lock.getReadCount() );
            assertEquals( "write count", 1, lock.getWriteCount() );
        }
        finally
        {
            tx.finish();
        }
        LockInfo[] locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.length );
    }

    private LockInfo getSingleLock()
    {
        LockInfo[] locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 1, locks.length );
        LockInfo lock = locks[0];
        assertNotNull( "null lock", lock );
        return lock;
    }

    private static AbstractGraphDatabase graphDb;

    @BeforeClass
    public static synchronized void startGraphDb()
    {
        graphDb = new EmbeddedGraphDatabase( "target" + File.separator + "var" + File.separator
                                             + ManagementBeansTest.class.getSimpleName() );
    }

    @AfterClass
    public static synchronized void stopGraphDb()
    {
        try
        {
            if ( graphDb != null ) graphDb.shutdown();
        }
        finally
        {
            graphDb = null;
        }
    }
}
