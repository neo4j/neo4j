/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Executes {@link WorkerCommand}s in another thread. Very useful for writing
 * tests which handles two simultaneous transactions and interleave them,
 * f.ex for testing locking and data visibility.
 * 
 * @author Mattias Persson
 *
 * @param <T>
 */
public abstract class OtherThreadExecutor<T>
{
    private final ExecutorService commandExecutor = newSingleThreadExecutor();
    private final T state;

    public OtherThreadExecutor( T initialState )
    {
        this.state = initialState;
    }

    public <R> Future<R> executeDontWait( final WorkerCommand<T, R> cmd ) throws Exception
    {
        return commandExecutor.submit( new Callable<R>()
        {
            @Override
            public R call()
            {
                return cmd.doWork( state );
            }
        } );
    }
    
    public <R> R execute( WorkerCommand<T, R> cmd ) throws Exception
    {
        return executeDontWait( cmd ).get();
    }

    public interface WorkerCommand<T, R>
    {
        R doWork( T state );
    }
}
