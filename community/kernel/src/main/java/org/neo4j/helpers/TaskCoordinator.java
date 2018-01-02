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
package org.neo4j.helpers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.neo4j.function.Factory;

/**
 * Represents a collection point for various {@link TaskControl} instances that need to be waited on and potentially
 * cancelled en mass. Instances of {@link TaskControl} acquired through the {@link #newInstance()} method can be
 * notified of cancellation with the semantics of {@link CancellationRequest}.
 */
public class TaskCoordinator implements Cancelable, Factory<TaskControl>
{
    private static final AtomicIntegerFieldUpdater<TaskCoordinator> TASKS =
            AtomicIntegerFieldUpdater.newUpdater( TaskCoordinator.class, "tasks" );
    private volatile boolean cancelled;
    @SuppressWarnings( "UnusedDeclaration"/*updated through the updater*/ )
    private volatile int tasks;
    private final long sleepTime;
    private final TimeUnit sleepUnit;

    public TaskCoordinator( long sleepTime, TimeUnit sleepUnit )
    {
        this.sleepTime = sleepTime;
        this.sleepUnit = sleepUnit;
    }

    @Override
    public void cancel()
    {
        cancelled = true;
    }

    public void awaitCompletion() throws InterruptedException
    {
        while ( tasks != 0 )
        {
            sleepUnit.sleep( sleepTime );
        }
    }

    @Override
    public TaskControl newInstance()
    {
        if ( cancelled )
        {
            throw new IllegalStateException( "This manager has already been cancelled." );
        }
        TASKS.incrementAndGet( this );
        return new TaskControl()
        {
            private volatile boolean closed;

            @Override
            public void close()
            {
                if ( !closed )
                {
                    closed = true;
                    TASKS.decrementAndGet( TaskCoordinator.this );
                }
            }

            @Override
            public boolean cancellationRequested()
            {
                return cancelled;
            }
        };
    }
}
