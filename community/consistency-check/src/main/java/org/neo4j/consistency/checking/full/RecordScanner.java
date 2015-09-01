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

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

public class RecordScanner<RECORD> extends ConsistencyCheckerTask
{
    private final ProgressListener progress;
    private final BoundedIterable<RECORD> store;
    private final RecordProcessor<RECORD> processor;
    private final Stage stage;
    private final IterableStore[] warmUpStores;
    private final CacheAccess cacheAccess;

    public RecordScanner( String name, Statistics statistics, int threads, BoundedIterable<RECORD> store,
            ProgressMonitorFactory.MultiPartBuilder builder, RecordProcessor<RECORD> processor, Stage stage,
            CacheAccess cacheAccess, IterableStore... warmUpStores )
    {
        super( name, statistics, threads );
        this.store = store;
        this.processor = processor;
        this.cacheAccess = cacheAccess;
        this.progress = builder.progressForPart( name, store.maxCount() );
        this.stage = stage;
        this.warmUpStores = warmUpStores;
    }

    @Override
    public void run()
    {
        statistics.reset();
        if ( warmUpStores != null )
        {
            for ( IterableStore store : warmUpStores )
            {
                store.warmUpCache();
            }
        }
        if ( !stage.isParallel() )
        {
            runSequential();
        }
        else
        {
            runParallel();
        }
        statistics.print( name );
    }

    public void runSequential()
    {
        try
        {
            int entryCount = 0;
            for ( RECORD record : store )
            {
                if ( !continueScanning )
                {
                    return;
                }
                processor.process( record );
                progress.set( entryCount++ );
            }
        }
        finally
        {
            try
            {
                store.close();
            }
            catch ( Exception e )
            {
                progress.failed( e );
                throw Exceptions.launderedException( e );
            }
            processor.close();
            progress.done();
        }
    }

    public void runParallel()
    {
        Workers<Worker<RECORD>> workers = new Workers<>( getClass().getSimpleName() );
        ArrayBlockingQueue<RECORD>[] recordQ = new ArrayBlockingQueue[numberOfThreads];
        for ( int threadId = 0; threadId < numberOfThreads; threadId++ )
        {
            recordQ[threadId] = new ArrayBlockingQueue<>( DefaultCacheAccess.DEFAULT_QUEUE_SIZE );
            workers.start( new Worker<>( recordQ[threadId], processor ) );
        }

        long recordsPerCPU = (store.maxCount() / numberOfThreads) + 1;
        cacheAccess.prepareForProcessingOfSingleStore( recordsPerCPU );
        int[] recsProcessed = new int[numberOfThreads];
        int qIndex = 0;
        int entryCount = 0;
        for ( RECORD record : store )
        {
            try
            {
                // do a round robin distribution to maintain physical locality
                recordQ[qIndex++].put( record );
                qIndex %= numberOfThreads;
                recsProcessed[qIndex]++;
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
            progress.set( entryCount++ );
        }
        progress.done();
        for ( Worker<RECORD> worker : workers )
        {
            worker.done();
        }
        workers.awaitAndThrowOnErrorStrict( RuntimeException.class );
    }

    private static class Worker<RECORD> extends RecordCheckWorker<RECORD>
    {
        private final RecordProcessor<RECORD> processor;

        Worker( ArrayBlockingQueue<RECORD> recordsQ, RecordProcessor<RECORD> processor )
        {
            super( recordsQ );
            this.processor = processor;
        }

        @Override
        protected void process( RECORD record )
        {
            processor.process( record );
        }
    }
}
