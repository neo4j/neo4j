/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
    private RecordDistributor()
    {
    }

    public static <RECORD> void distributeRecords(
            int numberOfThreads,
            String workerNames,
            int queueSize,
            Iterator<RECORD> records,
            final ProgressListener progress,
            RecordProcessor<RECORD> processor,
            QueueDistributor<RECORD> idDistributor )
    {
        if ( !records.hasNext() )
        {
            return;
        }

        @SuppressWarnings( "unchecked" )
        final ArrayBlockingQueue<RECORD>[] recordQ = new ArrayBlockingQueue[numberOfThreads];
        final Workers<RecordCheckWorker<RECORD>> workers = new Workers<>( workerNames );
        final AtomicInteger idGroup = new AtomicInteger( -1 );
        for ( int threadId = 0; threadId < numberOfThreads; threadId++ )
        {
            recordQ[threadId] = new ArrayBlockingQueue<>( queueSize );
            workers.start( new RecordCheckWorker<>( threadId, idGroup, recordQ[threadId], processor ) );
        }

        final int[] recsProcessed = new int[numberOfThreads];
        RecordConsumer<RECORD> recordConsumer = ( record, qIndex ) ->
        {
            recordQ[qIndex].put( record );
            recsProcessed[qIndex]++;
        };

        try
        {
            while ( records.hasNext() )
            {
                try
                {
                    // Put records into the queues using the queue distributor. Each Worker will pull and process.
                    RECORD record = records.next();
                    idDistributor.distribute( record, recordConsumer );
                    progress.add( 1 );
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // No more records to distribute, mark as done so that the workers will exit when no more records in queue.
            for ( RecordCheckWorker<RECORD> worker : workers )
            {
                worker.done();
            }

            workers.awaitAndThrowOnError();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Was interrupted while awaiting completion" );
        }
    }

    /**
     * Consumers records from a {@link QueueDistribution}, feeding into correct queue.
     */
    interface RecordConsumer<RECORD>
    {
        void accept( RECORD record, int qIndex ) throws InterruptedException;
    }

    public static long calculateRecordsPerCpu( long highId, int numberOfThreads )
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
