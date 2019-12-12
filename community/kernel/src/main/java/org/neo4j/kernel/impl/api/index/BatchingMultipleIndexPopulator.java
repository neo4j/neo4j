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
package org.neo4j.kernel.impl.api.index;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.neo4j.common.EntityType;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.FeatureToggles;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * A {@link MultipleIndexPopulator} that gathers all incoming updates from the {@link IndexStoreView} in batches of
 * size {@link #BATCH_SIZE_SCAN} and then flushes each batch from different thread using {@link ExecutorService executor}.
 * <p>
 * It is possible for concurrent updates from transactions to arrive while index population is in progress. Such
 * updates are inserted in the queue. When store scan notices that queue size has reached {@link #QUEUE_THRESHOLD} than
 * it drains all batched updates and waits for all submitted to the executor tasks to complete and flushes updates from
 * the queue using {@link MultipleIndexUpdater}. If queue size never reaches {@link #QUEUE_THRESHOLD} than all queued
 * concurrent updates are flushed after the store scan in {@link MultipleIndexPopulator#flipAfterPopulation(boolean)}.
 * <p>
 * Inner {@link ExecutorService executor} is shut down after the store scan completes.
 */
public class BatchingMultipleIndexPopulator extends MultipleIndexPopulator
{
    static final String AWAIT_TIMEOUT_MINUTES_NAME = "await_timeout_minutes";

    private static final String EOL = System.lineSeparator();

    private final int AWAIT_TIMEOUT_MINUTES = FeatureToggles.getInteger( getClass(), AWAIT_TIMEOUT_MINUTES_NAME, 30 );
    private final AtomicLong activeTasks = new AtomicLong();

    /**
     * Creates a new multi-threaded populator for the given store view.
     * @param storeView the view of the store as a visitable of nodes
     * @param logProvider the log provider
     * @param type entity type to populate
     * @param schemaState the schema state
     * @param jobScheduler the job scheduler
     */
    BatchingMultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider, EntityType type, SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore, JobScheduler jobScheduler )
    {
        super( storeView, logProvider, type, schemaState, indexStatisticsStore, jobScheduler );
    }

    /**
     * Creates a new multi-threaded populator with the specified thread pool.
     * <p>
     * <b>NOTE:</b> for testing only.
     * @param storeView the view of the store as a visitable of nodes
     * @param logProvider the log provider
     * @param schemaState the schema state
     * @param jobScheduler the job scheduler
     */
    BatchingMultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider, SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore, JobScheduler jobScheduler )
    {
        super( storeView, logProvider, EntityType.NODE, schemaState, indexStatisticsStore, jobScheduler );
    }

    @Override
    public StoreScan<IndexPopulationFailedKernelException> indexAllEntities()
    {
        StoreScan<IndexPopulationFailedKernelException> storeScan = super.indexAllEntities();
        return new BatchingStoreScan<>( storeScan );
    }

    @Override
    protected void flushAll()
    {
        super.flushAll();
        awaitCompletion();
    }

    @Override
    public String toString()
    {
        String updatesString = populations
                .stream()
                .map( population -> population.batchedUpdatesFromScan.size() + " updates" )
                .collect( joining( ", ", "[", "]" ) );

        return "BatchingMultipleIndexPopulator{activeTasks=" + activeTasks + ", " +
               "batchedUpdatesFromScan = " + updatesString + ", concurrentUpdateQueue = " + concurrentUpdateQueue.size() + "}";
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

            BooleanSupplier allSubmittedTasksCompleted = () -> activeTasks.get() == 0;
            Predicates.await( allSubmittedTasksCompleted, AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES );
        }
        catch ( TimeoutException e )
        {
            handleTimeout();
        }
    }

    /**
     * Insert the given batch of updates into the index defined by the given {@link IndexPopulation}.
     * Called from {@link MultipleIndexPopulator#flush(IndexPopulation)}.
     *
     * @param population the index population.
     */
    @Override
    void doFlush( IndexPopulation population )
    {
        activeTasks.incrementAndGet();
        List<IndexEntryUpdate<?>> batch = population.takeCurrentBatchFromScan();

        jobScheduler.schedule( Group.INDEX_POPULATION_WORK, () ->
        {
            try
            {
                String batchDescription = "EMPTY";
                if ( PRINT_DEBUG )
                {
                    if ( !batch.isEmpty() )
                    {
                        batchDescription = format( "[%d, %d - %d]", batch.size(), batch.get( 0 ).getEntityId(), batch.get( batch.size() - 1 ).getEntityId() );
                    }
                    log.info( "Applying scan batch %s", batchDescription );
                }
                population.populator.add( batch );
                if ( PRINT_DEBUG )
                {
                    log.info( "Applied scan batch %s", batchDescription );
                }
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

    private void handleTimeout()
    {
        throw new IllegalStateException( "Index population tasks were not able to complete in " +
                                         AWAIT_TIMEOUT_MINUTES + " minutes." + EOL + this + EOL + allStackTraces() );
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
     * A delegating {@link StoreScan} implementation that flushes all pending updates and terminates the executor after
     * the delegate store scan completes.
     *
     * @param <E> type of the exception this store scan might get.
     */
    private class BatchingStoreScan<E extends Exception> extends DelegatingStoreScan<E>
    {
        BatchingStoreScan( StoreScan<E> delegate )
        {
            super( delegate );
        }

        @Override
        public void run() throws E
        {
            super.run();
            log.info( "Completed node store scan. " +
                      "Flushing all pending updates." + EOL + BatchingMultipleIndexPopulator.this );
            flushAll();
        }
    }
}
