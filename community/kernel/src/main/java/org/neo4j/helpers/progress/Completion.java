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
package org.neo4j.helpers.progress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.ProcessFailureException;

public final class Completion
{
    private volatile Collection<Runnable> callbacks = new ArrayList<>();
    private final List<ProcessFailureException.Entry> processFailureCauses = new ArrayList<>();

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    void complete()
    {
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            Runnable[] targets;
            synchronized ( callbacks )
            {
                targets = callbacks.toArray( new Runnable[callbacks.size()] );
                this.callbacks = null;
            }
            for ( Runnable target : targets )
            {
                try
                {
                    target.run();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    void signalFailure( String part, Throwable e )
    {
        processFailureCauses.add( new ProcessFailureException.Entry( part, e ) );
        complete();
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    void notify( Runnable callback )
    {
        if ( callback == null )
        {
            throw new IllegalArgumentException( "callback may not be null" );
        }
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            synchronized ( callbacks )
            {
                if ( this.callbacks == callbacks )
                { // double checked locking
                    callbacks.add( callback );
                    callback = null; // we have not reached completion
                }
            }
        }
        if ( callback != null )
        { // we have already reached completion
            callback.run();
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void await( long timeout, TimeUnit unit )
            throws InterruptedException, TimeoutException, ProcessFailureException
    {
        CountDownLatch latch = null;
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            synchronized ( callbacks )
            {
                if ( this.callbacks == callbacks )
                { // double checked locking
                    callbacks.add( new CountDown( latch = new CountDownLatch( 1 ) ) );
                }
            }
        }
        if ( latch != null )
        { // await completion
            if ( !latch.await( timeout, unit ) )
            {
                throw new TimeoutException(
                        String.format( "Process did not complete within %d %s.", timeout, unit.name() ) );
            }
        }
        if ( !processFailureCauses.isEmpty() )
        {
            throw new ProcessFailureException( processFailureCauses );
        }
    }

    private static final class CountDown implements Runnable
    {
        private final CountDownLatch latch;

        CountDown( CountDownLatch latch )
        {
            this.latch = latch;
        }

        @Override
        public void run()
        {
            latch.countDown();
        }
    }
}
