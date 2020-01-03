/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.scheduler;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;

import org.neo4j.scheduler.DeferredExecutor;

/**
 * Buffers all tasks sent to it, and is able to replay those messages into
 * another Executor.
 * <p>
 * This will replay tasks in the order they are received.
 * <p>
 * You should also not use this executor, when there is a risk that it can be
 * subjected to an unbounded quantity of tasks, since the buffer keeps
 * all messages until it gets a chance to replay them.
 */
public class BufferingExecutor implements DeferredExecutor
{
    private final Queue<Runnable> buffer = new LinkedList<>();

    private volatile Executor realExecutor;

    public void satisfyWith( Executor executor )
    {
        synchronized ( this )
        {
            if ( realExecutor != null )
            {
                throw new RuntimeException( "real executor is already set. Cannot override" );
            }
            realExecutor = executor;
            replayBuffer();
        }
    }

    private void replayBuffer()
    {
        Runnable command = pollRunnable();
        while ( command != null )
        {
            realExecutor.execute( command );
            command = pollRunnable();
        }
    }

    private Runnable pollRunnable()
    {
        synchronized ( buffer )
        {
            return buffer.poll();
        }
    }

    private void queueRunnable( Runnable command )
    {
        synchronized ( buffer )
        {
            buffer.add( command );
        }
    }

    @Override
    public void execute( @Nonnull Runnable command )
    {
        // First do an unsynchronized check to see if a realExecutor is present
        if ( realExecutor != null )
        {
            realExecutor.execute( command );
            return;
        }

        // Now do a synchronized check to avoid race conditions
        synchronized ( this )
        {
            if ( realExecutor != null )
            {
                realExecutor.execute( command );
            }
            else
            {
                queueRunnable( command );
            }
        }
    }
}
