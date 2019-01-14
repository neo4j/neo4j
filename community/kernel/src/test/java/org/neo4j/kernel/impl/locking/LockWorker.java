/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
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
        {
            awaitFuture( future );
        }
        else
        {
            waitUntilWaiting();
        }
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
                state.client.acquireShared( LockTracer.NONE, NODE, resource );
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
                state.client.acquireExclusive( LockTracer.NONE, NODE, resource );
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

    private abstract static class AcquireLockCommand implements WorkerCommand<LockWorkerState, Void>
    {
        @Override
        public Void doWork( LockWorkerState state )
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
