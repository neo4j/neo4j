/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.util.StringLogger.LineLogger;
import org.neo4j.test.OtherThreadExecutor;

public class LockWorker extends OtherThreadExecutor<LockWorkerState>
{
    public LockWorker( String name, LockManager grabber )
    {
        super( name, new LockWorkerState( grabber ) );
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
    
    public Future<Void> getReadLock( final ResourceObject resource, final boolean wait ) throws Exception
    {
        return perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "+R " + resource + ", wait:" + wait );
                state.grabber.getReadLock( resource, state.tx );
                state.done();
            }
        }, wait );
    }

    public Future<Void> getWriteLock( final ResourceObject resource, final boolean wait ) throws Exception
    {
        return perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "+W " + resource + ", wait:" + wait );
                state.grabber.getWriteLock( resource, state.tx );
                state.done();
            }
        }, wait );
    }
    
    public void releaseReadLock( final ResourceObject resource ) throws Exception
    {
        perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "-R " + resource );
                state.grabber.releaseReadLock( resource, state.tx );
                state.done();
            }
        }, true );
    }
    
    public void releaseWriteLock( final ResourceObject resource ) throws Exception
    {
        perform( new AcquireLockCommand()
        {
            @Override
            protected void acquireLock( LockWorkerState state )
            {
                state.doing( "-W " + resource );
                state.grabber.releaseWriteLock( resource, state.tx );
                state.done();
            }
        }, true );
    }

    public boolean isLastGetLockDeadLock()
    {
        return state.deadlockOnLastWait;
    }
    
    @Override
    public boolean visit( LineLogger logger )
    {
        boolean result = super.visit( logger );
        logger.logLine( "What have I done up until now?" );
        for ( String op : state.completedOperations )
            logger.logLine( op );
        logger.logLine( "Doing right now:" );
        logger.logLine( state.doing );
        return result;
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

        protected abstract void acquireLock( LockWorkerState state );
    }
}
