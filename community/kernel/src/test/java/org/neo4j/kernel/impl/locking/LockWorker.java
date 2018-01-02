/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.logging.Logger;
import org.neo4j.test.OtherThreadExecutor;

import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

public class LockWorker extends OtherThreadExecutor<LockWorkerState>
{
    public LockWorker( String name, Locks locks )
    {
        super( name, new LockWorkerState( locks ) );
    }
    
    private Future<Void> perform( AcquireLockCommand acquireLockCommand, boolean wait ) throws Exception
    {
        Future<Void> future = executeDontWait( acquireLockCommand );
        if ( wait )
            awaitFuture( future );
        else
            waitUntilWaiting();
        return future;
    }
    
    public Future<Void> getReadLock( final long resource, final boolean wait ) throws Exception
    {
        return perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state ) throws AcquireLockTimeoutException
            {
                state.doing( "+R " + resource + ", wait:" + wait );
                state.client.acquireShared( NODE, resource );
                state.done();
            }
        }, wait );
    }

    public Future<Void> getWriteLock( final long resource, final boolean wait ) throws Exception
    {
        return perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state ) throws AcquireLockTimeoutException
            {
                state.doing( "+W " + resource + ", wait:" + wait );
                state.client.acquireExclusive( NODE, resource );
                state.done();
            }
        }, wait );
    }
    
    public void releaseReadLock( final long resource ) throws Exception
    {
        perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "-R " + resource );
                state.client.releaseShared( NODE, resource );
                state.done();
            }
        }, true );
    }
    
    public void releaseWriteLock( final long resource ) throws Exception
    {
        perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "-W " + resource );
                state.client.releaseExclusive( NODE, resource );
                state.done();
            }
        }, true );
    }

    public boolean isLastGetLockDeadLock()
    {
        return state.deadlockOnLastWait;
    }
    
    @Override
    public void dump( Logger logger )
    {
        super.dump( logger );
        logger.log( "What have I done up until now?" );
        for ( String op : state.completedOperations )
            logger.log( op );
        logger.log( "Doing right now:" );
        logger.log( state.doing );
    }
    
    public static ResourceObject newResourceObject( String name )
    {
        return new ResourceObject( name );
    }

    public static class ResourceObject
    {
        private final String name;

        ResourceObject( String name )
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }
    }
    
    private abstract static class AcquireLockCommand implements WorkerCommand<LockWorkerState, Void>
    {
        @Override
        public Void doWork( LockWorkerState state ) throws Exception
        {
            try
            {
                acquireLock( state );
                state.deadlockOnLastWait = false;
            }
            catch ( DeadlockDetectedException e )
            {
                state.deadlockOnLastWait = true;
            }
            return null;
        }

        protected abstract void acquireLock( LockWorkerState state ) throws AcquireLockTimeoutException;
    }
}
