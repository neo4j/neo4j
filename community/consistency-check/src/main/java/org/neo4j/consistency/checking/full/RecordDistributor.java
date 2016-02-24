/*
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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.consistency.checking.full.QueueDistribution.QueueDistributor;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

/**
 * Takes a stream of RECORDs and distributes them, via {@link BlockingQueue} onto multiple workers.
 */
public class RecordDistributor
{
    public static <RECORD> void distributeRecords(
            int numberOfThreads,
            String workerNames,
            int queueSize,
            Iterable<RECORD> records,
            ProgressListener progress,
            RecordProcessor<RECORD> processor,
            QueueDistributor<RECORD> idDistributor )
    {
        Iterator<RECORD> iterator = records.iterator();

        // Run the first record in the main thread since there are filters in some
        // checkers that change state on first and last record, state that may affect other concurrent
        // processors.
        if ( iterator.hasNext() )
        {
            processor.process( iterator.next() );
            progress.add( 1 );
        }
        else
        {
            // No need to set up a bunch of threads if there are no records to process anyway
            return;
        }

        final ArrayBlockingQueue<RECORD>[] recordQ = new ArrayBlockingQueue[numberOfThreads];
        final Workers<Worker<RECORD>> workers = new Workers<>( workerNames );
        final AtomicInteger idGroup = new AtomicInteger( -1 );
        for ( int threadId = 0; threadId < numberOfThreads; threadId++ )
        {
            recordQ[threadId] = new ArrayBlockingQueue<>( queueSize );
            workers.start( new Worker<>( threadId, idGroup, recordQ[threadId], processor ) );
        }

        final int[] recsProcessed = new int[numberOfThreads];
        RecordConsumer<RECORD> recordConsumer = new RecordConsumer<RECORD>()
        {
            @Override
            public void accept( RECORD record, int qIndex ) throws InterruptedException
            {
                recordQ[qIndex].put( record );
                recsProcessed[qIndex]++;
            }
        };

        RECORD last = null;
        while ( iterator.hasNext() )
        {
            try
            {
                // Put records into the queues using the queue distributor. Each Worker will pull and process.
                RECORD record = iterator.next();

                // Detect the last record and defer processing that until after all the others
                // since there are filters in some checkers that change state on first and last record,
                // state that may affect other concurrent processors.
                if ( !iterator.hasNext() )
                {
                    last = record;
                    break;
                }
                idDistributor.distribute( record, recordConsumer );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
            progress.add( 1 );
        }
        for ( Worker<RECORD> worker : workers )
        {
            worker.done();
        }
        try
        {
            workers.awaitAndThrowOnError( RuntimeException.class );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Was interrupted while awaiting completion" );
        }

        // Here we process the last record. Why? See comments above
        if ( last != null )
        {
            processor.process( last );
            progress.add( 1 );
        }
    }

    private static class Worker<RECORD> extends RecordCheckWorker<RECORD>
    {
        private final RecordProcessor<RECORD> processor;

        Worker( int id, AtomicInteger idGroup, BlockingQueue<RECORD> recordsQ, RecordProcessor<RECORD> processor )
        {
            super( id, idGroup, recordsQ );
            this.processor = processor;
        }

        @Override
        protected void process( RECORD record )
        {
            processor.process( record );
        }
    }

    /**
     * Consumers records from a {@link QueueDistribution}, feeding into correct queue.
     */
    interface RecordConsumer<RECORD>
    {
        void accept( RECORD record, int qIndex ) throws InterruptedException;
    }

    public static long calculateRecodsPerCpu( long highId, int numberOfThreads )
    {
        boolean hasRest = highId % numberOfThreads > 0;
        long result = highId / numberOfThreads;
        if ( hasRest )
        {
            result++;
        }
        return result;
    }
}
