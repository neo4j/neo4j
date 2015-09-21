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
package org.neo4j.consistency.checking.full;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.function.Function;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

/**
 * Takes a stream of RECORDs and distributes them, via {@link BlockingQueue} onto multiple workers.
 */
public class RecordDistributor
{
    public static <RECORD,WORKER extends RecordCheckWorker<RECORD>> void distributeRecords(
            int numberOfThreads,
            String workerNames,
            int queueSize,
            Function<BlockingQueue<RECORD>,WORKER> workerFactory,
            Iterable<RECORD> records,
            ProgressListener progress )
    {
        ArrayBlockingQueue<RECORD>[] recordQ = new ArrayBlockingQueue[numberOfThreads];
        Workers<WORKER> workers = new Workers<>( workerNames );
        for ( int threadId = 0; threadId < numberOfThreads; threadId++ )
        {
            recordQ[threadId] = new ArrayBlockingQueue<>( queueSize );
            workers.start( workerFactory.apply( recordQ[threadId] ) );
        }

        int[] recsProcessed = new int[numberOfThreads];
        int qIndex = 0;
        for ( RECORD record : records )
        {
            try
            {
                // Put records round-robin style into the queue of each thread, where a Worker
                // will sit and pull from and process.
                qIndex = (qIndex + 1)%numberOfThreads;
                recordQ[qIndex].put( record );
                recsProcessed[qIndex]++;
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
            progress.add( 1 );
        }
        for ( WORKER worker : workers )
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
    }
}
