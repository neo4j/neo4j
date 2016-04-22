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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.NamedThreadFactory.daemon;

/**
 * A {@link MultipleIndexPopulator} that gathers all incoming updates from the {@link IndexStoreView} in batches of
 * size {@link #BATCH_SIZE} and then flushes each batch from different thread using {@link ExecutorService executor}.
 * <p>
 * It is possible for concurrent updates from transactions to arrive while index population is in progress. Such
 * updates are inserted in the queue. When store scan notices that queue size has reached {@link #QUEUE_THRESHOLD} than
 * it drains all batched updates and waits for all submitted to the executor tasks to complete and flushes updates from
 * the queue using {@link MultipleIndexUpdater}. If queue size never reaches {@link #QUEUE_THRESHOLD} than all queued
 * concurrent updates are flushed after the store scan in {@link #flipAfterPopulation()}.
 * <p>
 * Inner {@link ExecutorService executor} is shut down after the store scan completes.
 */
public class BatchingMultipleIndexPopulator extends MultipleIndexPopulator
{
    public static final String QUEUE_THRESHOLD_NAME = "queue_threshold";
    static final String TASK_QUEUE_SIZE_NAME = "task_queue_size";
    static final String AWAIT_TIMEOUT_MINUTES_NAME = "await_timeout_minutes";
    static final String BATCH_SIZE_NAME = "batch_size";

    private static final String EOL = System.lineSeparator();
    private static final String FLUSH_THREAD_NAME_PREFIX = "Index Population Flush Thread";

    private final int QUEUE_THRESHOLD = FeatureToggles.getInteger( getClass(), QUEUE_THRESHOLD_NAME, 20_000 );
    private final int TASK_QUEUE_SIZE = FeatureToggles.getInteger( getClass(), TASK_QUEUE_SIZE_NAME,
            getNumberOfPopulationWorkers() * 2 );
    private final int AWAIT_TIMEOUT_MINUTES = FeatureToggles.getInteger( getClass(), AWAIT_TIMEOUT_MINUTES_NAME, 30 );
    private final int BATCH_SIZE = FeatureToggles.getInteger( getClass(), BATCH_SIZE_NAME, 10_000 );

    private final AtomicLong activeTasks = new AtomicLong();
    private final ExecutorService executor;
    private final Map<IndexPopulation,List<NodePropertyUpdate>> batchedUpdates = new HashMap<>();

    /**
     * Creates a new multi-threaded populator for the given store view.
     *
     * @param storeView the view of the store as a visitable of nodes
     * @param logProvider the log provider
     */
    public BatchingMultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider )
    {
        super( storeView, logProvider );
        this.executor = createThreadPool();
    }

    /**
     * Creates a new multi-threaded populator with the specified thread pool.
     * <p>
     * <b>NOTE:</b> for testing only.
     *
     * @param storeView the view of the store as a visitable of nodes
     * @param executor the thread pool to use for batched index insertions
     * @param logProvider the log provider
     */
    BatchingMultipleIndexPopulator( IndexStoreView storeView, ExecutorService executor, LogProvider logProvider )
    {
        super( storeView, logProvider );
        this.executor = executor;
    }

    @Override
    public StoreScan<IndexPopulationFailedKernelException> indexAllNodes()
    {
        StoreScan<IndexPopulationFailedKernelException> storeScan = super.indexAllNodes();
        return new BatchingStoreScan<>( storeScan );
    }

    @Override
    protected IndexPopulation createPopulation( IndexPopulator populator,
            IndexDescriptor descriptor, IndexConfiguration config, SchemaIndexProvider.Descriptor providerDescriptor,
            FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
    {
        return new BatchingIndexPopulation( populator, descriptor, config, providerDescriptor, flipper,
                failedIndexProxyFactory, indexUserDescription );
    }

    @Override
    protected void populateFromQueue( long currentlyIndexedNodeId )
    {
        if ( queue.size() >= QUEUE_THRESHOLD )
        {
            log.debug( "Populating from queue." + EOL + this );
            flushAll();
            awaitCompletion();
            super.populateFromQueue( currentlyIndexedNodeId );
            log.debug( "Drained queue and all batched updates." + EOL + this );
        }
    }

    @Override
    public String toString()
    {
        String updatesString = batchedUpdates.entrySet()
                .stream()
                .map( entry -> entry.getKey() + " - " + entry.getValue().size() + " updates" )
                .collect( joining( ", ", "[", "]" ) );

        return "BatchingMultipleIndexPopulator{activeTasks=" + activeTasks + ", executor=" + executor + ", " +
               "batchedUpdates = " + updatesString + ", queuedUpdates = " + queue.size() + "}";
    }

    /**
     * Awaits {@link #AWAIT_TIMEOUT_MINUTES} minutes for all previously submitted batch-flush tasks to complete.
     * Restores the interrupted status and exits normally when interrupted during waiting.
     *
     * @throws IllegalStateException if tasks did not complete in {@link #AWAIT_TIMEOUT_MINUTES} minutes.
     */
    private void awaitCompletion()
    {
        try
        {
            log.debug( "Waiting " + AWAIT_TIMEOUT_MINUTES + " minutes for all submitted and active " +
                       "flush tasks to complete." + EOL + this );

            Supplier<Boolean> allSubmittedTasksCompleted = () -> activeTasks.get() == 0;
            Predicates.await( allSubmittedTasksCompleted, AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES );
        }
        catch ( TimeoutException e )
        {
            handleTimeout();
        }
        catch ( InterruptedException e )
        {
            handleInterrupt();
        }
    }

    /**
     * Add given {@link NodePropertyUpdate update} to the list of updates already present for the given
     * {@link IndexPopulation population}. Flushes all updates if {@link #BATCH_SIZE} is reached.
     *
     * @param population the index population.
     * @param updates updates to add to the batch.
     */
    private void batchUpdate( IndexPopulation population, Collection<NodePropertyUpdate> updates )
    {
        List<NodePropertyUpdate> batch = batchedUpdates.computeIfAbsent( population, key -> newBatch() );
        batch.addAll( updates );
        flushIfNeeded( population, batch );
    }

    /**
     * Insert given list of updates to the index if {@link #BATCH_SIZE} is reached.
     *
     * @param population the index population.
     * @param batch the list of updates for the index.
     */
    private void flushIfNeeded( IndexPopulation population, List<NodePropertyUpdate> batch )
    {
        if ( batch.size() >= BATCH_SIZE )
        {
            batchedUpdates.remove( population );
            flush( population, batch );
        }
    }

    /**
     * Insert all batched updates into corresponding indexes.
     */
    private void flushAll()
    {
        Iterator<Map.Entry<IndexPopulation,List<NodePropertyUpdate>>> entries = batchedUpdates.entrySet().iterator();
        while ( entries.hasNext() )
        {
            Map.Entry<IndexPopulation,List<NodePropertyUpdate>> entry = entries.next();
            IndexPopulation population = entry.getKey();
            List<NodePropertyUpdate> updates = entry.getValue();
            entries.remove();
            if ( updates != null && !updates.isEmpty() )
            {
                flush( population, updates );
            }
        }
    }

    /**
     * Insert the given batch of updates into the index defined by the given {@link IndexDescriptor}.
     *
     * @param population the index population.
     * @param batch the list of updates to insert.
     */
    private void flush( IndexPopulation population, List<NodePropertyUpdate> batch )
    {
        activeTasks.incrementAndGet();

        executor.execute( () -> {
            try
            {
                population.populator.add( batch );
            }
            catch ( Throwable failure )
            {
                fail( population, failure );
            }
            finally
            {
                activeTasks.decrementAndGet();
            }
        } );
    }

    /**
     * Shuts down the executor waiting {@link #AWAIT_TIMEOUT_MINUTES} minutes for it's termination.
     * Restores the interrupted status and exits normally when interrupted during waiting.
     *
     * @param now <code>true</code> if {@link ExecutorService#shutdownNow()} should be used and <code>false</code> if
     * {@link ExecutorService#shutdown()} should be used.
     * @throws IllegalStateException if tasks did not complete in {@link #AWAIT_TIMEOUT_MINUTES} minutes.
     */
    private void shutdownExecutor( boolean now )
    {
        log.info( (now ? "Forcefully shutting" : "Shutting") + " down executor." + EOL + this );
        if ( now )
        {
            executor.shutdownNow();
        }
        else
        {
            executor.shutdown();
        }

        try
        {
            boolean tasksCompleted = executor.awaitTermination( AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES );
            if ( !tasksCompleted )
            {
                handleTimeout();
            }
        }
        catch ( InterruptedException e )
        {
            handleInterrupt();
        }
    }

    private void handleTimeout()
    {
        throw new IllegalStateException( "Index population tasks were not able to complete in " +
                                         AWAIT_TIMEOUT_MINUTES + " minutes." + EOL + this + EOL + allStackTraces() );
    }

    private void handleInterrupt()
    {
        Thread.currentThread().interrupt();
        log.warn( "Interrupted while waiting for index population tasks to complete." + EOL + this );
    }

    private List<NodePropertyUpdate> newBatch()
    {
        return new ArrayList<>( BATCH_SIZE );
    }

    private ExecutorService createThreadPool()
    {
        int threads = getNumberOfPopulationWorkers();
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( TASK_QUEUE_SIZE );
        ThreadFactory threadFactory = daemon( FLUSH_THREAD_NAME_PREFIX );
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor( threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
                rejectedExecutionHandler );
    }

    /**
     * Finds all threads and corresponding stack traces which can potentially cause the
     * {@link ExecutorService executor} to not terminate in {@link #AWAIT_TIMEOUT_MINUTES} minutes.
     *
     * @return thread dump as string.
     */
    private static String allStackTraces()
    {
        return Thread.getAllStackTraces()
                .entrySet()
                .stream()
                .map( entry -> Exceptions.stringify( entry.getKey(), entry.getValue() ) )
                .collect( joining() );
    }

    /**
     * Calculate number of workers that will perform index population
     *
     * @return number of threads that will be used for index population
     */
    private static int getNumberOfPopulationWorkers()
    {
        return Math.max( 2, Runtime.getRuntime().availableProcessors() - 1 );
    }

    /**
     * An {@link IndexPopulation} that does not insert updates one by one into the index but instead adds them to the
     * map containing batches of updates for each index.
     */
    private class BatchingIndexPopulation extends IndexPopulation
    {
        BatchingIndexPopulation( IndexPopulator populator, IndexDescriptor descriptor, IndexConfiguration config,
                SchemaIndexProvider.Descriptor providerDescriptor, FlippableIndexProxy flipper,
                FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
        {
            super( populator, descriptor, config, providerDescriptor, flipper, failedIndexProxyFactory,
                    indexUserDescription );
        }

        @Override
        protected void addApplicable( Collection<NodePropertyUpdate> updates ) throws IOException,
                IndexEntryConflictException
        {
            batchUpdate( this, updates );
        }
    }

    /**
     * A delegating {@link StoreScan} implementation that flushes all pending updates and terminates the executor after
     * the delegate store scan completes.
     *
     * @param <E> type of the exception this store scan might get.
     */
    private class BatchingStoreScan<E extends Exception> implements StoreScan<E>
    {
        final StoreScan<E> delegate;

        BatchingStoreScan( StoreScan<E> delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void run() throws E
        {
            try
            {
                delegate.run();
                log.info( "Completed node store scan. " +
                          "Flushing all pending deletes." + EOL + BatchingMultipleIndexPopulator.this );
                flushAll();
            }
            catch ( Throwable scanError )
            {
                try
                {
                    shutdownExecutor( true );
                }
                catch ( Throwable error )
                {
                    scanError.addSuppressed( error );
                }
                throw scanError;
            }
            shutdownExecutor( false );
        }

        @Override
        public void stop()
        {
            delegate.stop();
        }

        @Override
        public PopulationProgress getProgress()
        {
            return delegate.getProgress();
        }
    }
}
