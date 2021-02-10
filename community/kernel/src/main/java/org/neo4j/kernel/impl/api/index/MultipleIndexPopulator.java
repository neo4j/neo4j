/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.eclipse.collections.impl.utility.ArrayIterate.contains;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * There are two ways data is fed to this multi-populator:
 * <ul>
 * <li>A {@link StoreScan} is created through {@link #createStoreScan(PageCacheTracer)}. The store scan is started by
 * {@link StoreScan#run(StoreScan.ExternalUpdatesCheck)}, which is a blocking call and will scan the entire store and generate
 * updates that are fed into the {@link IndexPopulator populators}. Only a single call to this
 * method should be made during the life time of a {@link MultipleIndexPopulator} and should be called by the
 * same thread instantiating this instance.</li>
 * <li>{@link #queueConcurrentUpdate(IndexEntryUpdate)} which queues updates which will be read by the thread currently executing
 * the store scan and incorporated into that data stream. Calls to this method may come from any number
 * of concurrent threads.</li>
 * </ul>
 * <p>
 * Usage of this class should be something like:
 * <ol>
 * <li>Instantiation.</li>
 * <li>One or more calls to {@link #addPopulator(IndexPopulator, IndexDescriptor, FlippableIndexProxy, FailedIndexProxyFactory, String)}.</li>
 * <li>Call to {@link #create(PageCursorTracer)} to create data structures and files to start accepting updates.</li>
 * <li>Call to {@link #createStoreScan(PageCacheTracer)} and {@link StoreScan#run(StoreScan.ExternalUpdatesCheck)}(blocking call).</li>
 * <li>While all nodes are being indexed, calls to {@link #queueConcurrentUpdate(IndexEntryUpdate)} are accepted.</li>
 * <li>Call to {@link #flipAfterStoreScan(boolean, PageCursorTracer)} after successful population, or {@link #cancel(Throwable, PageCursorTracer)} if not</li>
 * </ol>
 * <p>
 * It is possible for concurrent updates from transactions to arrive while index population is in progress. Such
 * updates are inserted in the {@link #queueConcurrentUpdate(IndexEntryUpdate) queue}. When store scan notices that
 * queue size has reached {@link #queueThreshold} then it drains all batched updates and waits for all job scheduler
 * tasks to complete and flushes updates from the queue using {@link MultipleIndexUpdater}. If queue size never reaches
 * {@link #queueThreshold} than all queued concurrent updates are flushed after the store scan in
 * {@link MultipleIndexPopulator#flipAfterStoreScan(boolean, PageCursorTracer)}.
 * <p>
 */
public class MultipleIndexPopulator implements StoreScan.ExternalUpdatesCheck
{
    private static final String MULTIPLE_INDEX_POPULATOR_TAG = "multipleIndexPopulator";
    private static final String POPULATION_WORK_FLUSH_TAG = "populationWorkFlush";
    private static final String EOL = System.lineSeparator();

    private final int queueThreshold;
    final int batchMaxByteSizeScan;
    private final boolean printDebug;

    // Concurrency queue since multiple concurrent threads may enqueue updates into it. It is important for this queue
    // to have fast #size() method since it might be drained in batches
    private final Queue<IndexEntryUpdate<?>> concurrentUpdateQueue = new LinkedBlockingQueue<>();
    private final AtomicLong concurrentUpdateQueueByteSize = new AtomicLong();

    // Populators are added into this list. The same thread adding populators will later call #createStoreScan.
    // Multiple concurrent threads might fail individual populations.
    // Failed populations are removed from this list while iterating over it.
    private final List<IndexPopulation> populations = new CopyOnWriteArrayList<>();

    private final AtomicLong activeTasks = new AtomicLong();
    private final IndexStoreView storeView;
    private final NodePropertyAccessor propertyAccessor;
    private final LogProvider logProvider;
    private final Log log;
    private final EntityType type;
    private final SchemaState schemaState;
    private final IndexStatisticsStore indexStatisticsStore;
    private final PhaseTracker phaseTracker;
    private final JobScheduler jobScheduler;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;
    private StoreScan<IndexPopulationFailedKernelException> storeScan;
    private final TokenNameLookup tokenNameLookup;
    private final PageCacheTracer cacheTracer;
    private final String databaseName;
    private final Subject subject;

    public MultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider, EntityType type, SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore, JobScheduler jobScheduler, TokenNameLookup tokenNameLookup, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker, String databaseName, Subject subject, Config config )
    {
        this.storeView = storeView;
        this.cursorTracer = cacheTracer.createPageCursorTracer( MULTIPLE_INDEX_POPULATOR_TAG );
        this.memoryTracker = memoryTracker;
        this.propertyAccessor = storeView.newPropertyAccessor( cursorTracer, memoryTracker );
        this.logProvider = logProvider;
        this.log = logProvider.getLog( IndexPopulationJob.class );
        this.type = type;
        this.schemaState = schemaState;
        this.indexStatisticsStore = indexStatisticsStore;
        this.phaseTracker = new LoggingPhaseTracker( logProvider.getLog( IndexPopulationJob.class ) );
        this.jobScheduler = jobScheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.cacheTracer = cacheTracer;
        this.databaseName = databaseName;
        this.subject = subject;

        this.printDebug = config.get( GraphDatabaseInternalSettings.index_population_print_debug );
        this.queueThreshold = config.get( GraphDatabaseInternalSettings.index_population_queue_threshold );
        this.batchMaxByteSizeScan = config.get( GraphDatabaseInternalSettings.index_population_batch_max_byte_size ).intValue();
    }

    IndexPopulation addPopulator( IndexPopulator populator, IndexDescriptor indexDescriptor, FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
    {
        IndexPopulation population = createPopulation( populator, indexDescriptor, flipper, failedIndexProxyFactory, indexUserDescription );
        populations.add( population );
        return population;
    }

    private IndexPopulation createPopulation( IndexPopulator populator, IndexDescriptor indexDescriptor, FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
    {
        return new IndexPopulation( populator, indexDescriptor, flipper, failedIndexProxyFactory, indexUserDescription );
    }

    boolean hasPopulators()
    {
        return !populations.isEmpty();
    }

    public void create( PageCursorTracer cursorTracer )
    {
        forEachPopulation( population ->
        {
            log.info( "Index population started: [%s]", population.indexUserDescription );
            population.create();
        }, cursorTracer );
    }

    StoreScan<IndexPopulationFailedKernelException> createStoreScan( PageCacheTracer cacheTracer )
    {
        int[] entityTokenIds = entityTokenIds();
        int[] propertyKeyIds = propertyKeyIds();
        IntPredicate propertyKeyIdFilter = propertyKeyId -> contains( propertyKeyIds, propertyKeyId );

        if ( type == EntityType.RELATIONSHIP )
        {
            storeScan = storeView.visitRelationships( entityTokenIds, propertyKeyIdFilter, new EntityPopulationVisitor(), null, false, true, cacheTracer,
                    memoryTracker );
        }
        else
        {
            storeScan =
                    storeView.visitNodes( entityTokenIds, propertyKeyIdFilter, new EntityPopulationVisitor(), null, false, true, cacheTracer, memoryTracker );
        }
        storeScan.setPhaseTracker( phaseTracker );
        return new BatchingStoreScan<>( storeScan );
    }

    /**
     * Queues an update to be fed into the index populators. These updates come from changes being made
     * to storage while a concurrent scan is happening to keep populators up to date with all latest changes.
     *
     * @param update {@link IndexEntryUpdate} to queue.
     */
    void queueConcurrentUpdate( IndexEntryUpdate<?> update )
    {
        concurrentUpdateQueue.add( update );
        concurrentUpdateQueueByteSize.addAndGet( update.roughSizeOfUpdate() );
    }

    /**
     * Cancel all {@link IndexPopulation index populations}, putting the indexes in {@link InternalIndexState#FAILED failed state}.
     * To repopulate them they will need to be dropped and recreated.
     *
     * @param failure the cause.
     */
    public void cancel( Throwable failure, PageCursorTracer cursorTracer )
    {
        for ( IndexPopulation population : populations )
        {
            cancel( population, failure, cursorTracer );
        }
    }

    /**
     * Cancel a single {@link IndexPopulation index population}, putting the index in {@link InternalIndexState#FAILED failed state}.
     * To repopulate the index it needs to be dropped and recreated.
     *
     * @param population Index population to cancel.
     * @param failure the cause.
     */
    protected void cancel( IndexPopulation population, Throwable failure, PageCursorTracer cursorTracer )
    {
        if ( !removeFromOngoingPopulations( population ) )
        {
            return;
        }

        // If the cause of index population failure is a conflict in a (unique) index, the conflict is the failure
        if ( failure instanceof IndexPopulationFailedKernelException )
        {
            Throwable cause = failure.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                failure = cause;
            }
        }

        log.error( format( "Failed to populate index: [%s]", population.indexUserDescription ), failure );

        // The flipper will have already flipped to a failed index context here, but
        // it will not include the cause of failure, so we do another flip to a failed
        // context that does.

        // The reason for having the flipper transition to the failed index context in the first
        // place is that we would otherwise introduce a race condition where updates could come
        // in to the old context, if something failed in the job we send to the flipper.
        IndexPopulationFailure indexPopulationFailure = failure( failure );
        population.cancel( indexPopulationFailure );
        try
        {
            population.populator.markAsFailed( indexPopulationFailure.asString() );
            population.populator.close( false, cursorTracer );
        }
        catch ( Throwable e )
        {
            log.error( format( "Unable to close failed populator for index: [%s]",
                    population.indexUserDescription ), e );
        }
    }

    @VisibleForTesting
    MultipleIndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor, PageCursorTracer cursorTracer )
    {
        Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> updaters = new HashMap<>();
        forEachPopulation( population ->
        {
            IndexUpdater updater = population.populator.newPopulatingUpdater( accessor, cursorTracer );
            updaters.put( population.schema(), Pair.of( population, updater ) );
        }, cursorTracer );
        return new MultipleIndexUpdater( this, updaters, logProvider, cursorTracer );
    }

    /**
     * Close this {@link MultipleIndexPopulator multiple index populator}.
     * This means population job has finished, successfully or unsuccessfully and resources can be released.
     *
     * Note that {@link IndexPopulation index populations} cannot be closed. Instead, the underlying
     * {@link IndexPopulator index populator} is closed by {@link #flipAfterStoreScan(boolean, PageCursorTracer)},
     * {@link #cancel(IndexPopulation, Throwable, PageCursorTracer)} or {@link #stop(IndexPopulation, PageCursorTracer)}.
     */
    public void close()
    {
        phaseTracker.stop();
        propertyAccessor.close();
        cursorTracer.close();
    }

    void resetIndexCounts( PageCursorTracer cursorTracer )
    {
        forEachPopulation( this::resetIndexCountsForPopulation, cursorTracer );
    }

    private void resetIndexCountsForPopulation( IndexPopulation indexPopulation )
    {
        indexStatisticsStore.replaceStats( indexPopulation.indexId, new IndexSample( 0, 0, 0 ) );
    }

    /**
     * This concludes a successful index population.
     *
     * The last updates will be applied to every index,
     * tell {@link IndexPopulator index populators} that scan has been completed,
     * {@link IndexStatisticsStore index statistics store} will be updated with {@link IndexSample index samples},
     * {@link SchemaState schema cache} will be cleared,
     * {@link IndexPopulator index populators} will be closed and
     * {@link IndexProxy index proxy} will be {@link FlippableIndexProxy#flip(Callable, FailedIndexProxyFactory) flipped}
     * to {@link OnlineIndexProxy online}, given that nothing goes wrong.
     *
     * @param verifyBeforeFlipping Whether to verify deferred constraints before flipping index proxy. This is used by batch inserter.
     */
    void flipAfterStoreScan( boolean verifyBeforeFlipping, PageCursorTracer cursorTracer )
    {
        for ( IndexPopulation population : populations )
        {
            try
            {
                population.scanCompleted( cursorTracer );
                population.flip( verifyBeforeFlipping, cursorTracer );
            }
            catch ( Throwable t )
            {
                cancel( population, t, cursorTracer );
            }
        }
    }

    private int[] propertyKeyIds()
    {
        return populations.stream().flatMapToInt( this::propertyKeyIds ).distinct().toArray();
    }

    private IntStream propertyKeyIds( IndexPopulation population )
    {
        return IntStream.of( population.schema().getPropertyIds() );
    }

    private int[] entityTokenIds()
    {
        return populations.stream().flatMapToInt( population -> Arrays.stream( population.schema().getEntityTokenIds() ) ).toArray();
    }

    /**
     * Stop all {@link IndexPopulation index populations}, closing backing {@link IndexPopulator index populators},
     * keeping them in {@link InternalIndexState#POPULATING populating state}.
     */
    public void stop( PageCursorTracer cursorTracer )
    {
        forEachPopulation( population -> this.stop( population, cursorTracer ), cursorTracer );
    }

    /**
     * Close specific {@link IndexPopulation index population}, closing backing {@link IndexPopulator index populator},
     * keeping it in {@link InternalIndexState#POPULATING populating state}.
     * @param indexPopulation {@link IndexPopulation} to stop.
     */
    void stop( IndexPopulation indexPopulation, PageCursorTracer cursorTracer )
    {
        indexPopulation.disconnectAndStop( cursorTracer );
    }

    /**
     * Stop population of given {@link IndexPopulation} and drop the index.
     * @param indexPopulation {@link IndexPopulation} to drop.
     */
    void dropIndexPopulation( IndexPopulation indexPopulation )
    {
        indexPopulation.disconnectAndDrop();
    }

    private boolean removeFromOngoingPopulations( IndexPopulation indexPopulation )
    {
        return populations.remove( indexPopulation );
    }

    @Override
    public boolean needToApplyExternalUpdates()
    {
        int queueSize = concurrentUpdateQueue.size();
        return (queueSize > 0 && queueSize >= queueThreshold) || concurrentUpdateQueueByteSize.get() >= batchMaxByteSizeScan;
    }

    @Override
    public void applyExternalUpdates( long currentlyIndexedNodeId )
    {
        if ( concurrentUpdateQueue.isEmpty() )
        {
            return;
        }

        if ( printDebug )
        {
            log.info( "Populating from queue at %d", currentlyIndexedNodeId );
        }

        long updateByteSizeDrained = 0;
        try ( MultipleIndexUpdater updater = newPopulatingUpdater( propertyAccessor, cursorTracer ) )
        {
            do
            {
                // no need to check for null as nobody else is emptying this queue
                IndexEntryUpdate<?> update = concurrentUpdateQueue.poll();
                // Since updates can be added concurrently with us draining the queue simply setting the value to 0
                // after drained will not be 100% synchronized with the queue contents and could potentially cause a large
                // drift over time. Therefore each update polled from the queue will subtract its size instead.
                updateByteSizeDrained += update != null ? update.roughSizeOfUpdate() : 0;
                if ( update != null && update.getEntityId() <= currentlyIndexedNodeId )
                {
                    updater.process( update );
                }
                if ( printDebug )
                {
                    log.info( "Applied %s from queue" + (update == null ? null : update.describe( tokenNameLookup ) ) );
                }
            }
            while ( !concurrentUpdateQueue.isEmpty() );
            concurrentUpdateQueueByteSize.addAndGet( -updateByteSizeDrained );
        }
        if ( printDebug )
        {
            log.info( "Done applying updates from queue" );
        }
    }

    private void forEachPopulation( ThrowingConsumer<IndexPopulation,Exception> action, PageCursorTracer cursorTracer )
    {
        for ( IndexPopulation population : populations )
        {
            try
            {
                action.accept( population );
            }
            catch ( Throwable failure )
            {
                cancel( population, failure, cursorTracer );
            }
        }
    }

    @Override
    public String toString()
    {
        String updatesString = populations
                .stream()
                .map( Object::toString )
                .collect( joining( ", ", "[", "]" ) );

        return "MultipleIndexPopulator{activeTasks=" + activeTasks + ", " +
                "batchedUpdatesFromScan = " + updatesString + ", concurrentUpdateQueue = " + concurrentUpdateQueue.size() + "}";
    }

    public static class MultipleIndexUpdater implements IndexUpdater
    {
        private final Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> populationsWithUpdaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final Log log;
        private final PageCursorTracer cursorTracer;

        MultipleIndexUpdater( MultipleIndexPopulator multipleIndexPopulator,
                Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> populationsWithUpdaters, LogProvider logProvider, PageCursorTracer cursorTracer )
        {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.populationsWithUpdaters = populationsWithUpdaters;
            this.log = logProvider.getLog( getClass() );
            this.cursorTracer = cursorTracer;
        }

        @Override
        public void process( IndexEntryUpdate<?> update )
        {
            Pair<IndexPopulation,IndexUpdater> pair = populationsWithUpdaters.get( update.indexKey().schema() );
            if ( pair != null )
            {
                IndexPopulation population = pair.first();
                IndexUpdater updater = pair.other();

                try
                {
                    population.populator.includeSample( update );
                    updater.process( update );
                }
                catch ( Throwable t )
                {
                    try
                    {
                        updater.close();
                    }
                    catch ( Throwable ce )
                    {
                        log.error( format( "Failed to close index updater: [%s]", updater ), ce );
                    }
                    populationsWithUpdaters.remove( update.indexKey().schema() );
                    multipleIndexPopulator.cancel( population, t, cursorTracer );
                }
            }
        }

        @Override
        public void close()
        {
            for ( Pair<IndexPopulation,IndexUpdater> pair : populationsWithUpdaters.values() )
            {
                IndexPopulation population = pair.first();
                IndexUpdater updater = pair.other();

                try
                {
                    updater.close();
                }
                catch ( Throwable t )
                {
                    multipleIndexPopulator.cancel( population, t, cursorTracer );
                }
            }
            populationsWithUpdaters.clear();
        }
    }

    public class IndexPopulation implements SchemaDescriptorSupplier
    {
        public final IndexPopulator populator;
        final FlippableIndexProxy flipper;
        private final long indexId;
        private final IndexDescriptor indexDescriptor;
        private final FailedIndexProxyFactory failedIndexProxyFactory;
        private final String indexUserDescription;
        private boolean populationOngoing = true;
        private final ReentrantLock populatorLock = new ReentrantLock();

        IndexPopulation( IndexPopulator populator, IndexDescriptor indexDescriptor, FlippableIndexProxy flipper,
                FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
        {
            this.populator = populator;
            this.indexDescriptor = indexDescriptor;
            this.indexId = indexDescriptor.getId();
            this.flipper = flipper;
            this.failedIndexProxyFactory = failedIndexProxyFactory;
            this.indexUserDescription = indexUserDescription;
        }

        private void cancel( IndexPopulationFailure failure )
        {
            flipper.flipTo( new FailedIndexProxy( indexDescriptor, indexUserDescription, populator, failure, indexStatisticsStore, logProvider ) );
        }

        void create() throws IOException
        {
            populatorLock.lock();
            try
            {
                if ( populationOngoing )
                {
                    populator.create();
                }
            }
            finally
            {
                populatorLock.unlock();
            }
        }

        /**
         * Disconnect this single {@link IndexPopulation index population} from ongoing multiple index population
         * and close {@link IndexPopulator index populator}, leaving it in {@link InternalIndexState#POPULATING populating state}.
         */
        void disconnectAndStop( PageCursorTracer cursorTracer )
        {
            disconnect( () -> populator.close( false, cursorTracer ) );
        }

        /**
         * Disconnect this single {@link IndexPopulation index population} from ongoing multiple index population
         * and {@link IndexPopulator#drop() drop} the index.
         */
        void disconnectAndDrop()
        {
            disconnect( populator::drop );
        }

        private void disconnect( Runnable specificPopulatorOperation )
        {
            populatorLock.lock();
            try
            {
                if ( populationOngoing )
                {
                    // First of all remove this population from the list of ongoing populations so that it won't receive more updates.
                    // This is good because closing the populator may wait for an opportunity to perform the close, among the incoming writes to it.
                    removeFromOngoingPopulations( this );
                    specificPopulatorOperation.run();
                    resetIndexCountsForPopulation( this );
                    populationOngoing = false;
                }
            }
            finally
            {
                populatorLock.unlock();
            }
        }

        void flip( boolean verifyBeforeFlipping, PageCursorTracer cursorTracer ) throws FlipFailedKernelException
        {
            phaseTracker.enterPhase( PhaseTracker.Phase.FLIP );
            flipper.flip( () ->
            {
                populatorLock.lock();
                try
                {
                    if ( populationOngoing )
                    {
                        applyExternalUpdates( Long.MAX_VALUE );
                        if ( populations.contains( IndexPopulation.this ) )
                        {
                            if ( verifyBeforeFlipping )
                            {
                                populator.verifyDeferredConstraints( propertyAccessor );
                            }
                            IndexSample sample = populator.sample( cursorTracer );
                            indexStatisticsStore.replaceStats( indexId, sample );
                            populator.close( true, cursorTracer );
                            schemaState.clear();
                            return true;
                        }
                    }
                    return false;
                }
                finally
                {
                    logCompletionMessage();
                    populationOngoing = false;
                    populatorLock.unlock();
                }
            }, failedIndexProxyFactory );
            removeFromOngoingPopulations( this );
        }

        private void logCompletionMessage()
        {
            log.info( "Index creation finished for index [%s].", indexUserDescription );
        }

        @Override
        public SchemaDescriptor schema()
        {
            return indexDescriptor.schema();
        }

        @Override
        public String userDescription( TokenNameLookup tokenNameLookup )
        {
            return indexUserDescription;
        }

        void scanCompleted( PageCursorTracer cursorTracer ) throws IndexEntryConflictException
        {
            IndexPopulator.PopulationWorkScheduler populationWorkScheduler = new IndexPopulator.PopulationWorkScheduler()
            {
                @Override
                public <T> JobHandle<T> schedule( IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job )
                {
                    var description = descriptionSupplier.getJobDescription( indexDescriptor.getName() );
                    var jobMonitoringParams = new JobMonitoringParams( subject, databaseName, description );
                    return jobScheduler.schedule( Group.INDEX_POPULATION_WORK, jobMonitoringParams, job );
                }
            };

            populator.scanCompleted( phaseTracker, populationWorkScheduler, cursorTracer );
        }

        PopulationProgress progress( PopulationProgress storeScanProgress )
        {
            return populator.progress( storeScanProgress );
        }
    }

    private class EntityPopulationVisitor implements Visitor<List<EntityUpdates>,
            IndexPopulationFailedKernelException>
    {
        @Override
        public boolean visit( List<EntityUpdates> updates )
        {
            try ( var cursorTracer = cacheTracer.createPageCursorTracer( POPULATION_WORK_FLUSH_TAG ) )
            {
                addFromScan( updates, cursorTracer );
            }
            long lastEntityId = updates.get( updates.size() - 1 ).getEntityId();
            if ( printDebug )
            {
                log.info( "Added scan updates for entities %d-%d", updates.get( 0 ).getEntityId(), lastEntityId );
            }
            return false;
        }

        private void addFromScan( List<EntityUpdates> entityUpdates, PageCursorTracer cursorTracer )
        {
            // This is called from a full store node scan, meaning that all node properties are included in the
            // EntityUpdates object. Therefore no additional properties need to be loaded.
            Map<IndexPopulation,List<IndexEntryUpdate<IndexPopulation>>> updates = new HashMap<>( populations.size() );
            for ( EntityUpdates update : entityUpdates )
            {
                for ( IndexEntryUpdate<IndexPopulation> indexUpdate : update.forIndexKeys( populations ) )
                {
                    IndexPopulation population = indexUpdate.indexKey();
                    population.populator.includeSample( indexUpdate );
                    updates.computeIfAbsent( population, p -> new ArrayList<>() ).add( indexUpdate );
                }
            }
            for ( Map.Entry<IndexPopulation,List<IndexEntryUpdate<IndexPopulation>>> entry : updates.entrySet() )
            {
                try
                {
                    entry.getKey().populator.add( entry.getValue(), cursorTracer );
                }
                catch ( Throwable e )
                {
                    cancel( entry.getKey(), e, cursorTracer );
                }
            }
        }
    }

    protected static class DelegatingStoreScan<E extends Exception> implements StoreScan<E>
    {
        private final StoreScan<E> delegate;

        DelegatingStoreScan( StoreScan<E> delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void run( ExternalUpdatesCheck externalUpdatesCheck ) throws E
        {
            delegate.run( externalUpdatesCheck );
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

        @Override
        public void setPhaseTracker( PhaseTracker phaseTracker )
        {
            delegate.setPhaseTracker( phaseTracker );
        }
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
        public void run( ExternalUpdatesCheck externalUpdatesCheck ) throws E
        {
            super.run( externalUpdatesCheck );
            log.info( "Completed node store scan. " +
                      "Flushing all pending updates." + EOL + MultipleIndexPopulator.this );
        }
    }
}
