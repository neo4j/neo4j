/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

/**
 * Confirms that a nested {@link Transaction} can grab locks with its
 * explicit methods: {@link Transaction#acquireReadLock(org.neo4j.graphdb.PropertyContainer) acquireReadLock}
 * and {@link Transaction#acquireWriteLock(org.neo4j.graphdb.PropertyContainer) acquireWriteLock}.
 */
public class NestedTransactionLocksIT
{
    private ImpermanentGraphDatabase db;
    
    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private WorkerCommand<Void, Lock> acquireWriteLock( final Node resource )
    {
        return new WorkerCommand<Void, Lock>()
        {
            @Override
            public Lock doWork( Void state )
            {
                Transaction tx = db.beginTx();
                return tx.acquireWriteLock( resource );
            }
        };
    }
    
    @Test
    public void nestedTransactionCanAcquireLocksFromTransactionObject() throws Exception
    {
        // given
        Node resource = db.getReferenceNode();
        Transaction realTx = db.beginTx();
        Transaction nestedTx = db.beginTx();
        assertNotSame( realTx, nestedTx );
        OtherThreadExecutor<Void> otherTx = new OtherThreadExecutor<Void>( "other thread", null );
        
        // when
        Lock lock = nestedTx.acquireWriteLock( resource );
        Future<Lock> future = otherTx.executeDontWait( acquireWriteLock( resource ) );
        otherTx.waitUntilWaiting();
        
        // then
        try
        {
            future.get( 1, SECONDS );
            fail( "The nested transaction seems to not have acquired the lock" );
        }
        catch ( TimeoutException e )
        {   // Good
        }
        lock.release();
        assertNotNull( future.get() );
        otherTx.shutdown();
    }
}
