/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

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

    private AtomicInteger writerTaskCounter = new AtomicInteger();

    @Override
    public synchronized void merge( IndexWriter writer, MergeTrigger trigger, boolean newMergesFound )
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
                writerTaskCounter.incrementAndGet();
                PooledConcurrentMergePool.mergeThreadsPool.submit( mergeTask( mergeThread ) );
                success = true;
            }
            finally
            {
                if ( !success )
                {
                    writerTaskCounter.decrementAndGet();
                    writer.mergeFinish( merge );
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

    AtomicInteger getWriterTaskCounter()
    {
        return writerTaskCounter;
    }

    private Runnable mergeTask( MergeThread mergeThread )
    {
        return new MergeTask( mergeThread, writerTaskCounter );
    }

    private void waitForAllTasks()
    {
        while ( writerTaskCounter.get() > 0 )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }

    private static class PooledConcurrentMergePool
    {
        private static final ExecutorService mergeThreadsPool =
                new ThreadPoolExecutor( 0, getMaximumPoolSize(), 60L, TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>( POOL_QUEUE_CAPACITY ),
                        new NamedThreadFactory( "Lucene-Merge", true ), new ThreadPoolExecutor.CallerRunsPolicy() );

        private static int getMaximumPoolSize()
        {
            return Math.max( 1, Math.min( POOL_MINIMUM_THREADS, Runtime.getRuntime().availableProcessors() / 2 ) );
        }
    }

    private class MergeTask implements Runnable
    {
        private final MergeThread mergeThread;
        private final AtomicInteger taskCounter;

        MergeTask( MergeThread mergeThread, AtomicInteger taskCounter )
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
                taskCounter.decrementAndGet();
            }
        }
    }
}
