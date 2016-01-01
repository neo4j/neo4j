/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Writer;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.WriterFactory;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Queue of I/O jobs. A job is basically: "write the contents of ByteBuffer B to channel C starting at position P"
 * Calls to public (interface) methods that this class exposes are assumed to be single-threaded.
 */
public class IoQueue implements WriterFactory
{
    private final ExecutorService executor;
    private final JobMonitor jobMonitor = new JobMonitor();
    private final WriterFactory delegateFactory;

    public IoQueue( int maxIOThreads, WriterFactory delegateFactory )
    {
        this( Executors.newFixedThreadPool( maxIOThreads, new NamedThreadFactory( "IoQueue I/O thread" ) ),
                delegateFactory );
    }

    IoQueue( ExecutorService executor, WriterFactory delegateFactory )
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
        executor.shutdown();
        awaitEverythingWritten();
        try
        {
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

}
