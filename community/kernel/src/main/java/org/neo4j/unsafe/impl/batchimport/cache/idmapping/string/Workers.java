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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.neo4j.helpers.collection.Iterators;

/**
 * Utility for running a handful of {@link Runnable} in parallel, each in its own thread.
 * {@link Runnable} instances are {@link #start(Runnable) added and started} and the caller can
 * {@link #await()} them all to finish, returning a {@link Throwable error} if any thread encountered one so
 * that the caller can decide how to handle that error. Or caller can use {@link #awaitAndThrowOnError()}
 * where error from any worker would be thrown from that method.
 *
 * It's basically like using an {@link ExecutorService}, but without that "baggage" and an easier usage
 * and less code in the scenario described above.
 *
 * @param <R> type of workers
 */
public class Workers<R extends Runnable> implements Iterable<R>
{
    private final List<Worker> workers = new ArrayList<>();
    private final String names;

    public Workers( String names )
    {
        this.names = names;
    }

    /**
     * Starts a thread to run {@code toRun}. Returns immediately.
     *
     * @param toRun worker to start and run among potentially other workers.
     */
    public void start( R toRun )
    {
        Worker worker = new Worker( names + "-" + workers.size(), toRun );
        worker.start();
        workers.add( worker );
    }

    public Throwable await() throws InterruptedException
    {
        Throwable error = null;
        for ( Worker worker : workers )
        {
            Throwable anError = worker.await();
            if ( error == null )
            {
                error = anError;
            }
        }
        return error;
    }

    public Throwable awaitStrict()
    {
        try
        {
            return await();
        }
        catch ( InterruptedException e )
        {
            throw handleInterrupted( e );
        }
    }

    public void awaitAndThrowOnError() throws InterruptedException
    {
        Throwable error = await();
        if ( error != null )
        {
            throw new RuntimeException( error );
        }
    }

    public void awaitAndThrowOnErrorStrict( )
    {
        try
        {
            awaitAndThrowOnError();
        }
        catch ( InterruptedException e )
        {
            throw handleInterrupted( e );
        }
    }

    private RuntimeException handleInterrupted( InterruptedException e )
    {
        Thread.interrupted();
        return new RuntimeException( "Got interrupted while awaiting workers (" + names + ") to complete", e );
    }

    @Override
    public Iterator<R> iterator()
    {
        return Iterators.map( worker -> worker.toRun, workers.iterator() );
    }

    private class Worker extends Thread
    {
        private volatile Throwable error;
        private final R toRun;

        Worker( String name, R toRun )
        {
            super( name );
            this.toRun = toRun;
        }

        @Override
        public void run()
        {
            try
            {
                toRun.run();
            }
            catch ( Throwable t )
            {
                error = t;
                throw new RuntimeException( t );
            }
        }

        protected synchronized Throwable await() throws InterruptedException
        {
            join();
            return error;
        }
    }
}
