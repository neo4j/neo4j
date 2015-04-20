/*
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.unsafe.impl.batchimport.executor.DynamicTaskExecutor;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Writer;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Queue of I/O jobs. A job is basically: "write the contents of ByteBuffer B to channel C starting at position P"
 * Calls to public (interface) methods that this class exposes are assumed to be single-threaded.
 */
public class IoQueue implements WriterFactory
{
    private final TaskExecutor<Void> executor;
    private final JobMonitor jobMonitor = new JobMonitor();
    private final WriterFactory delegateFactory;

    public IoQueue( int initialProcessorCount, int maxProcessors, int queueSize, WriterFactory delegateFactory )
    {
        this( new DynamicTaskExecutor<Void>( initialProcessorCount, maxProcessors, queueSize,
                DynamicTaskExecutor.DEFAULT_PARK_STRATEGY, "IoQueue I/O thread" ), delegateFactory );
    }

    IoQueue( TaskExecutor<Void> executor, WriterFactory delegateFactory )
    {
        this.executor = executor;
        this.delegateFactory = delegateFactory;
    }

    @Override
    public Writer create( StoreChannel channel, Monitor monitor )
    {
        Writer writer = delegateFactory.create( channel, monitor );
        WriteQueue queue = new WriteQueue( executor, jobMonitor);
        return new Funnel( writer, queue );
    }

    @Override
    public void awaitEverythingWritten()
    {
        long endTime = System.currentTimeMillis()+MINUTES.toMillis( 10 );
        while ( jobMonitor.hasActiveJobs() )
        {
            executor.assertHealthy();
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }

            if ( System.currentTimeMillis() > endTime )
            {
                throw new RuntimeException( "Didn't finish within designated time" );
            }
        }
    }

    @Override
    public void shutdown()
    {
        executor.shutdown( true );
    }

    @Override
    public int numberOfProcessors()
    {
        return executor.numberOfProcessors();
    }

    @Override
    public boolean incrementNumberOfProcessors()
    {
        return executor.incrementNumberOfProcessors();
    }

    @Override
    public boolean decrementNumberOfProcessors()
    {
        return executor.decrementNumberOfProcessors();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + delegateFactory + ", threads:" + executor.numberOfProcessors() + "]";
    }
}
