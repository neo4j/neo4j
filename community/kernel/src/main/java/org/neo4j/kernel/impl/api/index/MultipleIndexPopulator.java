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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.util.FeatureToggles;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.contains;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * {@link IndexPopulator} that allow population of multiple indexes during one iteration.
 * Performs operations by calling corresponding operations of particular index populators.
 *
 * There are two ways data is fed to this multi-populator:
 * <ul>
 * <li>{@link #indexAllNodes()}, which is a blocking call and will scan the entire store and
 * and generate updates that are fed into the {@link IndexPopulator populators}. Only a single call to this
 * method should be made during the life time of a {@link MultipleIndexPopulator} and should be called by the
 * same thread instantiating this instance.</li>
 * <li>{@link #queue(IndexEntryUpdate)} which queues updates which will be read by the thread currently executing
 * {@link #indexAllNodes()} and incorporated into that data stream. Calls to this method may come from any number
 * of concurrent threads.</li>
 * </ul>
 *
 * Usage of this class should be something like:
 * <ol>
 * <li>Instantiation.</li>
 * <li>One or more calls to {@link #addPopulator(IndexPopulator, long, IndexMeta, FlippableIndexProxy,
 * FailedIndexProxyFactory, String)}.</li>
 * <li>Call to {@link #create()} to create data structures and files to start accepting updates.</li>
 * <li>Call to {@link #indexAllNodes()} (blocking call).</li>
 * <li>While all nodes are being indexed, calls to {@link #queue(IndexEntryUpdate)} are accepted.</li>
 * <li>Call to {@link #flipAfterPopulation()} after successful population, or {@link #fail(Throwable)} if not</li>
 * </ol>
 */
public class MultipleIndexPopulator implements IndexPopulator
{
    public static final String QUEUE_THRESHOLD_NAME = "queue_threshold";
    static final String BATCH_SIZE_NAME = "batch_size";

    private final int QUEUE_THRESHOLD = FeatureToggles.getInteger( getClass(), QUEUE_THRESHOLD_NAME, 20_000 );
    private final int BATCH_SIZE = FeatureToggles.getInteger( BatchingMultipleIndexPopulator.class, BATCH_SIZE_NAME, 10_000 );

    // Concurrency queue since multiple concurrent threads may enqueue updates into it. It is important for this queue
    // to have fast #size() method since it might be drained in batches
    protected final Queue<IndexEntryUpdate<?>> queue = new LinkedBlockingQueue<>();

    // Populators are added into this list. The same thread adding populators will later call #indexAllNodes.
    // Multiple concurrent threads might fail individual populations.
    // Failed populations are removed from this list while iterating over it.
    final List<IndexPopulation> populations = new CopyOnWriteArrayList<>();

    private final IndexStoreView storeView;
    private final LogProvider logProvider;
    protected final Log log;
    private final SchemaState schemaState;
    private StoreScan<IndexPopulationFailedKernelException> storeScan;

    public MultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider, SchemaState schemaState )
    {
        this.storeView = storeView;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( IndexPopulationJob.class );
        this.schemaState = schemaState;
    }

    IndexPopulation addPopulator(
            IndexPopulator populator,
            long indexId,
            IndexMeta indexMeta,
            FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory,
            String indexUserDescription )
    {
        IndexPopulation population =
                createPopulation( populator, indexId, indexMeta, flipper, failedIndexProxyFactory, indexUserDescription );
        populations.add( population );
        return population;
    }

    protected IndexPopulation createPopulation( IndexPopulator populator, long indexId, IndexMeta indexMeta,
            FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
    {
        return new IndexPopulation( populator, indexId, indexMeta, flipper, failedIndexProxyFactory, indexUserDescription );
    }

    boolean hasPopulators()
    {
        return !populations.isEmpty();
    }

    @Override
    public void create()
    {
        forEachPopulation( population ->
        {
            log.info( "Index population started: [%s]", population.indexUserDescription );
            population.create();
        } );
    }

    @Override
    public void drop()
    {
        throw new UnsupportedOperationException( "Can't drop indexes from this populator implementation" );
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        throw new UnsupportedOperationException( "Can't populate directly using this populator implementation. " );
    }

    public StoreScan<IndexPopulationFailedKernelException> indexAllNodes()
    {
        int[] labelIds = labelIds();
        int[] propertyKeyIds = propertyKeyIds();
        IntPredicate propertyKeyIdFilter = propertyKeyId -> contains( propertyKeyIds, propertyKeyId );

        storeScan = storeView.visitNodes( labelIds, propertyKeyIdFilter, new NodePopulationVisitor(), null, false );
        return new DelegatingStoreScan<IndexPopulationFailedKernelException>( storeScan )
        {
            @Override
            public void run() throws IndexPopulationFailedKernelException
            {
                super.run();
                flushAll();
            }
        };
    }

    /**
     * Queues an update to be fed into the index populators. These updates come from changes being made
     * to storage while a concurrent scan is happening to keep populators up to date with all latest changes.
     *
     * @param update {@link IndexEntryUpdate} to queue.
     */
    public void queue( IndexEntryUpdate<?> update )
    {
        queue.add( update );
    }

    /**
     * Called if forced failure from the outside
     *
     * @param failure index population failure.
     */
    public void fail( Throwable failure )
    {
        for ( IndexPopulation population : populations )
        {
            fail( population, failure );
        }
    }

    protected void fail( IndexPopulation population, Throwable failure )
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
        population.flipToFailed( indexPopulationFailure );
        try
        {
            population.populator.markAsFailed( indexPopulationFailure.asString() );
            population.populator.close( false );
        }
        catch ( Throwable e )
        {
            log.error( format( "Unable to close failed populator for index: [%s]",
                    population.indexUserDescription ), e );
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor )
    {
        throw new UnsupportedOperationException( "Should not be called directly" );
    }

    @Override
    public MultipleIndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> updaters = new HashMap<>();
        forEachPopulation( population ->
        {
            IndexUpdater updater = population.populator.newPopulatingUpdater( accessor );
            updaters.put( population.schema(), Pair.of( population, updater ) );
        } );
        return new MultipleIndexUpdater( this, updaters, logProvider );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        forEachPopulation( population -> population.populator.close( populationCompletedSuccessfully ) );
    }

    @Override
    public void markAsFailed( String failure )
    {
        throw new UnsupportedOperationException( "Multiple index populator can't be marked as failed." );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        throw new UnsupportedOperationException( "Multiple index populator can't perform index sampling." );
    }

    @Override
    public IndexSample sampleResult()
    {
        throw new UnsupportedOperationException( "Multiple index populator can't perform index sampling." );
    }

    void resetIndexCounts()
    {
        forEachPopulation( this::resetIndexCountsForPopulation );
    }

    private void resetIndexCountsForPopulation( IndexPopulation indexPopulation )
    {
        storeView.replaceIndexCounts( indexPopulation.indexId, 0, 0, 0 );
    }

    void flipAfterPopulation()
    {
        for ( IndexPopulation population : populations )
        {
            try
            {
                population.flip();
            }
            catch ( Throwable t )
            {
                fail( population, t );
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

    private int[] labelIds()
    {
        return populations.stream().mapToInt( population -> population.schema().keyId() ).toArray();
    }

    public void cancel()
    {
        forEachPopulation( this::cancelIndexPopulation );
    }

    void cancelIndexPopulation( IndexPopulation indexPopulation )
    {
        try
        {
            indexPopulation.cancel();
        }
        catch ( IOException e )
        {
            fail( indexPopulation, e );
        }
    }

    private boolean removeFromOngoingPopulations( IndexPopulation indexPopulation )
    {
        return populations.remove( indexPopulation );
    }

    void populateFromQueueBatched( long currentlyIndexedNodeId )
    {
        if ( isQueueThresholdReached() )
        {
            populateFromQueue( currentlyIndexedNodeId );
        }
    }

    private boolean isQueueThresholdReached()
    {
        return queue.size() >= QUEUE_THRESHOLD;
    }

    protected void populateFromQueue( long currentlyIndexedNodeId )
    {
        populateFromQueueIfAvailable( currentlyIndexedNodeId );
    }

    void flushAll()
    {
        populations.forEach( this::flush );
    }

    protected void flush( IndexPopulation population )
    {
        try
        {
            population.populator.add( population.takeCurrentBatch() );
        }
        catch ( Throwable failure )
        {
            fail( population, failure );
        }
    }

    private void populateFromQueueIfAvailable( long currentlyIndexedNodeId )
    {
        if ( !queue.isEmpty() )
        {
            try ( MultipleIndexUpdater updater = newPopulatingUpdater( storeView ) )
            {
                do
                {
                    // no need to check for null as nobody else is emptying this queue
                    IndexEntryUpdate<?> update = queue.poll();
                    storeScan.acceptUpdate( updater, update, currentlyIndexedNodeId );
                }
                while ( !queue.isEmpty() );
            }
        }
    }

    private void forEachPopulation( ThrowingConsumer<IndexPopulation,Exception> action )
    {
        for ( IndexPopulation population : populations )
        {
            try
            {
                action.accept( population );
            }
            catch ( Throwable failure )
            {
                fail( population, failure );
            }
        }
    }

    public static class MultipleIndexUpdater implements IndexUpdater
    {
        private final Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> populationsWithUpdaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final Log log;

        MultipleIndexUpdater( MultipleIndexPopulator multipleIndexPopulator,
                Map<SchemaDescriptor,Pair<IndexPopulation,IndexUpdater>> populationsWithUpdaters, LogProvider logProvider )
        {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.populationsWithUpdaters = populationsWithUpdaters;
            this.log = logProvider.getLog( getClass() );
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
                    multipleIndexPopulator.fail( population, t );
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
                    multipleIndexPopulator.fail( population, t );
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
        private final IndexMeta indexMeta;
        private final IndexCountsRemover indexCountsRemover;
        private final FailedIndexProxyFactory failedIndexProxyFactory;
        private final String indexUserDescription;
        private boolean populationOngoing = true;
        private final ReentrantLock populatorLock = new ReentrantLock();

        List<IndexEntryUpdate<?>> batchedUpdates;

        IndexPopulation(
                IndexPopulator populator,
                long indexId,
                IndexMeta indexMeta,
                FlippableIndexProxy flipper,
                FailedIndexProxyFactory failedIndexProxyFactory,
                String indexUserDescription )
        {
            this.populator = populator;
            this.indexId = indexId;
            this.indexMeta = indexMeta;
            this.flipper = flipper;
            this.failedIndexProxyFactory = failedIndexProxyFactory;
            this.indexUserDescription = indexUserDescription;
            this.indexCountsRemover = new IndexCountsRemover( storeView, indexId );
            this.batchedUpdates = new ArrayList<>( BATCH_SIZE );
        }

        private void flipToFailed( IndexPopulationFailure failure )
        {
            flipper.flipTo( new FailedIndexProxy( indexMeta, indexUserDescription, populator, failure, indexCountsRemover, logProvider ) );
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

        void cancel() throws IOException
        {
            populatorLock.lock();
            try
            {
                if ( populationOngoing )
                {
                    populator.close( false );
                    resetIndexCountsForPopulation( this );
                    removeFromOngoingPopulations( this );
                    populationOngoing = false;
                }
            }
            finally
            {
                populatorLock.unlock();
            }
        }

        private void onUpdate( IndexEntryUpdate<?> update )
        {
            populator.includeSample( update );
            if ( batch( update ) )
            {
                flush( this );
            }
        }

        void flip() throws FlipFailedKernelException
        {
            flipper.flip( () ->
            {
                populatorLock.lock();
                try
                {
                    if ( populationOngoing )
                    {
                        populator.add( takeCurrentBatch() );
                        populateFromQueueIfAvailable( Long.MAX_VALUE );
                        if ( populations.contains( IndexPopulation.this ) )
                        {
                            IndexSample sample = populator.sampleResult();
                            storeView.replaceIndexCounts( indexId, sample.uniqueValues(), sample.sampleSize(), sample.indexSize() );
                            populator.close( true );
                            schemaState.clear();
                            return true;
                        }
                    }
                    return false;
                }
                finally
                {
                    populationOngoing = false;
                    populatorLock.unlock();
                }
            }, failedIndexProxyFactory );
            removeFromOngoingPopulations( this );
            logCompletionMessage();
        }

        private void logCompletionMessage()
        {
            InternalIndexState postPopulationState = flipper.getState();
            String messageTemplate = isIndexPopulationOngoing( postPopulationState )
                                     ? "Index created. Starting data checks. Index [%s] is %s."
                                     : "Index creation finished. Index [%s] is %s.";
            log.info( messageTemplate, indexUserDescription, postPopulationState.name() );
        }

        private boolean isIndexPopulationOngoing( InternalIndexState postPopulationState )
        {
            return InternalIndexState.POPULATING == postPopulationState;
        }

        @Override
        public SchemaDescriptor schema()
        {
            return indexMeta.indexDescriptor().schema();
        }

        public boolean batch( IndexEntryUpdate<?> update )
        {
            batchedUpdates.add( update );
            return batchedUpdates.size() >= BATCH_SIZE;
        }

        Collection<IndexEntryUpdate<?>> takeCurrentBatch()
        {
            if ( batchedUpdates.isEmpty() )
            {
                return Collections.emptyList();
            }
            Collection<IndexEntryUpdate<?>> batch = batchedUpdates;
            batchedUpdates = new ArrayList<>( BATCH_SIZE );
            return batch;
        }
    }

    private class NodePopulationVisitor implements Visitor<NodeUpdates,
            IndexPopulationFailedKernelException>
    {
        @Override
        public boolean visit( NodeUpdates updates )
        {
            add( updates );
            populateFromQueueBatched( updates.getNodeId() );
            return false;
        }

        private void add( NodeUpdates updates )
        {
            // This is called from a full store node scan, meaning that all node properties are included in the
            // NodeUpdates object. Therefore no additional properties need to be loaded.
            for ( IndexEntryUpdate<IndexPopulation> indexUpdate : updates.forIndexKeys( populations ) )
            {
                indexUpdate.indexKey().onUpdate( indexUpdate );
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
        public void run() throws E
        {
            delegate.run();
        }

        @Override
        public void stop()
        {
            delegate.stop();
        }

        @Override
        public void acceptUpdate( MultipleIndexUpdater updater, IndexEntryUpdate<?> update, long currentlyIndexedNodeId )
        {
            delegate.acceptUpdate( updater, update, currentlyIndexedNodeId );
        }

        @Override
        public PopulationProgress getProgress()
        {
            return delegate.getProgress();
        }
    }
}
