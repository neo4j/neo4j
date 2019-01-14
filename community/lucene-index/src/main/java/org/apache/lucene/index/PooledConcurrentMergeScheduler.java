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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.util.FeatureToggles;

/**
 * Lucene indexes merge scheduler that execute merges in a thread pool instead of starting separate thread for each
 * merge. Each writer have it's own scheduler but all of them use shared pool.
 *
 * Current implementation is minimalistic in a number of things it knows about lucene internals to simplify
 * migrations overhead. It wraps up lucene internal merge tasks and execute them in a common thread pool.
 * In case if pool and queue exhausted merge will be performed in callers thread.
 * Since we cant rely on lucene per writer merge threads we need to perform writer tasks counting ourselves to prevent
 * cases while writer will be closed in the middle of merge and will wait for all writer related merges to complete
 * before allowing close of writer scheduler.
 */
public class PooledConcurrentMergeScheduler extends ConcurrentMergeScheduler
{
    private static final int POOL_QUEUE_CAPACITY =
            FeatureToggles.getInteger( PooledConcurrentMergeScheduler.class, "pool.queue.capacity", 100 );
    private static final int POOL_MINIMUM_THREADS =
            FeatureToggles.getInteger( PooledConcurrentMergeScheduler.class, "pool.minimum.threads", 4 );
    private static final int POOL_MAXIMUM_THREADS =
            FeatureToggles.getInteger( PooledConcurrentMergeScheduler.class, "pool.maximum.threads", 10 );

    private final LongAdder writerTaskCounter = new LongAdder();

    @Override
    public void merge( IndexWriter writer, MergeTrigger trigger, boolean newMergesFound )
            throws IOException
    {
        while ( true )
        {
            MergePolicy.OneMerge merge = writer.getNextMerge();
            if ( merge == null )
            {
                return;
            }
            boolean success = false;
            try
            {
                MergeThread mergeThread = getMergeThread( writer, merge );
                writerTaskCounter.increment();
                PooledConcurrentMergePool.mergeThreadsPool.submit( mergeTask( mergeThread ) );
                success = true;
            }
            finally
            {
                if ( !success )
                {
                    writer.mergeFinish( merge );
                    writerTaskCounter.decrement();
                }
            }
        }
    }

    @Override
    public void close()
    {
        waitForAllTasks();
        super.close();
    }

    @Override
    protected void updateMergeThreads()
    {
        //noop
    }

    @Override
    void removeMergeThread()
    {
        // noop
    }

    long getWriterTaskCount()
    {
        return writerTaskCounter.longValue();
    }

    private Runnable mergeTask( MergeThread mergeThread )
    {
        return new MergeTask( mergeThread, writerTaskCounter );
    }

    private void waitForAllTasks()
    {
        try
        {
            Predicates.await( () -> writerTaskCounter.longValue() == 0, 10, TimeUnit.MINUTES, 10, TimeUnit.MILLISECONDS );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private static class PooledConcurrentMergePool
    {
        private static final ExecutorService mergeThreadsPool =
                new ThreadPoolExecutor( POOL_MINIMUM_THREADS, getMaximumPoolSize(), 60L, TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>( POOL_QUEUE_CAPACITY ),
                        new NamedThreadFactory( "Lucene-Merge", true ), new ThreadPoolExecutor.CallerRunsPolicy() );

        private static int getMaximumPoolSize()
        {
            return Math.max( POOL_MAXIMUM_THREADS, Runtime.getRuntime().availableProcessors() );
        }
    }

    private static class MergeTask implements Runnable
    {
        private final MergeThread mergeThread;
        private final LongAdder taskCounter;

        MergeTask( MergeThread mergeThread, LongAdder taskCounter )
        {
            this.mergeThread = mergeThread;
            this.taskCounter = taskCounter;
        }

        @Override
        public void run()
        {
            try
            {
                mergeThread.run();
            }
            finally
            {
                taskCounter.decrement();
            }
        }
    }
}
