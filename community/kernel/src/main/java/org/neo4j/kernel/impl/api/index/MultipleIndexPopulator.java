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
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

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
 * <li>{@link #queue(NodePropertyUpdate)} which queues updates which will be read by the thread currently executing
 * {@link #indexAllNodes()} and incorporated into that data stream. Calls to this method may come from any number
 * of concurrent threads.</li>
 * </ul>
 *
 * Usage of this class should be something like:
 * <ol>
 * <li>Instantiation.</li>
 * <li>One or more calls to {@link #addPopulator(IndexPopulator, IndexDescriptor, Descriptor, IndexConfiguration,
 * FlippableIndexProxy, FailedIndexProxyFactory, String)}.</li>
 * <li>Call to {@link #create()} to create data structures and files to start accepting updates.</li>
 * <li>Call to {@link #indexAllNodes()} (blocking call).</li>
 * <li>While all nodes are being indexed, calls to {@link #queue(NodePropertyUpdate)} are accepted.</li>
 * <li>Call to {@link #flipAfterPopulation()} after successful population, or {@link #fail(Throwable)} if not</li>
 * </ol>
 */
public class MultipleIndexPopulator implements IndexPopulator
{

    public static final String QUEUE_THRESHOLD_NAME = "queue_threshold";
    private final int QUEUE_THRESHOLD = FeatureToggles.getInteger( getClass(), QUEUE_THRESHOLD_NAME, 20_000 );

    // Concurrency queue since multiple concurrent threads may enqueue updates into it. It is important for this queue
    // to have fast #size() method since it might be drained in batches
    protected final Queue<NodePropertyUpdate> queue = new LinkedBlockingQueue<>();

    // Populators are added into this list. The same thread adding populators will later call #indexAllNodes.
    // Multiple concurrent threads might fail individual populations.
    // Failed populations are removed from this list while iterating over it.
    private final List<IndexPopulation> populations = new CopyOnWriteArrayList<>();

    private final IndexStoreView storeView;
    private final LogProvider logProvider;
    protected final Log log;
    private StoreScan<IndexPopulationFailedKernelException> storeScan;

    public MultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider )
    {
        this.storeView = storeView;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( IndexPopulationJob.class );
    }

    public IndexPopulation addPopulator(
            IndexPopulator populator,
            IndexDescriptor descriptor,
            Descriptor providerDescriptor, IndexConfiguration config,
            FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory,
            String indexUserDescription )
    {
        IndexPopulation population = createPopulation( populator, descriptor, config, providerDescriptor, flipper,
                failedIndexProxyFactory, indexUserDescription );
        populations.add( population );
        return population;
    }

    protected IndexPopulation createPopulation( IndexPopulator populator, IndexDescriptor descriptor,
            IndexConfiguration config, Descriptor providerDescriptor, FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory, String indexUserDescription )
    {
        return new IndexPopulation( populator, descriptor, config, providerDescriptor, flipper, failedIndexProxyFactory,
                indexUserDescription );
    }

    public boolean hasPopulators()
    {
        return !populations.isEmpty();
    }

    @Override
    public void create()
    {
        forEachPopulation( population -> {
            log.info( "Index population started: [%s]", population.indexUserDescription );
            population.populator.create();
        } );
    }

    @Override
    public void drop() throws IOException
    {
        throw new UnsupportedOperationException( "Can't drop indexes from this populator implementation" );
    }

    @Override
    public void add( Collection<NodePropertyUpdate> updates )
    {
        throw new UnsupportedOperationException( "Can't populate directly using this populator implementation. " );
    }

    public StoreScan<IndexPopulationFailedKernelException> indexAllNodes()
    {
        int[] labelIds = labelIds();
        int[] propertyKeyIds = propertyKeyIds();
        IntPredicate propertyKeyIdFilter = (propertyKeyId) -> contains( propertyKeyIds, propertyKeyId );

        storeScan = storeView.visitNodes( labelIds, propertyKeyIdFilter, new NodePopulationVisitor(), null );
        storeScan.configure(populations);
        return storeScan;
    }

    /**
     * Queues an update to be fed into the index populators. These updates come from changes being made
     * to storage while a concurrent scan is happening to keep populators up to date with all latest changes.
     *
     * @param update {@link NodePropertyUpdate} to queue.
     */
    public void queue( NodePropertyUpdate update )
    {
        queue.add( update );
    }

    /**
     * Called if forced failure from the outside
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
        boolean removed = populations.remove( population );
        if ( !removed )
        {
            return;
        }

        // If the cause of index population failure is a conflict in a (unique) index, the conflict is the
        // failure
        // TODO do we need this?
        if ( failure instanceof IndexPopulationFailedKernelException )
        {
            Throwable cause = failure.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                failure = cause;
            }
        }

        // Index conflicts are expected (for unique indexes) so we don't need to log them.
        if ( !(failure instanceof IndexEntryConflictException) /*TODO: && this is a unique index...*/ )
        {
            log.error( format( "Failed to populate index: [%s]", population.indexUserDescription ), failure );
        }

        // The flipper will have already flipped to a failed index context here, but
        // it will not include the cause of failure, so we do another flip to a failed
        // context that does.

        // The reason for having the flipper transition to the failed index context in the first
        // place is that we would otherwise introduce a race condition where updates could come
        // in to the old context, if something failed in the job we send to the flipper.
        population.flipToFailed( failure );
        try
        {
            population.populator.markAsFailed( failure( failure ).asString() );
            population.populator.close( false );
        }
        catch ( Throwable e )
        {
            log.error( format( "Unable to close failed populator for index: [%s]",
                    population.indexUserDescription ), e );
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        throw new UnsupportedOperationException( "Should not be called directly" );
    }

    public void verifyAllDeferredConstraints( PropertyAccessor accessor )
    {
        forEachPopulation( population -> population.populator.verifyDeferredConstraints( accessor ) );
    }

    @Override
    public MultipleIndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        Map<IndexPopulation,IndexUpdater> populationsWithUpdaters = new HashMap<>();
        forEachPopulation( population -> {
            IndexUpdater updater = population.populator.newPopulatingUpdater( accessor );
            populationsWithUpdaters.put( population, updater );
        } );
        return new MultipleIndexUpdater( this, populationsWithUpdaters, logProvider );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        forEachPopulation( population -> population.populator.close( populationCompletedSuccessfully ) );
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        throw new UnsupportedOperationException( "Multiple index populator can't be marked as failed." );
    }

    @Override
    public void includeSample( NodePropertyUpdate update )
    {
        throw new UnsupportedOperationException( "Multiple index populator can't perform index sampling." );
    }

    @Override
    public void configureSampling( boolean onlineSampling )
    {
        throw new UnsupportedOperationException( "Multiple index populator can't be configured." );
    }

    @Override
    public IndexSample sampleResult()
    {
        throw new UnsupportedOperationException( "Multiple index populator can't perform index sampling." );
    }

    public void replaceIndexCounts( long uniqueElements, long maxUniqueElements, long indexSize )
    {
        forEachPopulation( population ->
                storeView.replaceIndexCounts( population.descriptor, uniqueElements, maxUniqueElements, indexSize ) );
    }

    public void flipAfterPopulation()
    {
        for ( IndexPopulation population : populations )
        {
            try
            {
                population.flip();
                populations.remove( population );
            }
            catch ( Throwable t )
            {
                fail( population, t );
            }
        }
    }

    private int[] propertyKeyIds()
    {
        return populations.stream().mapToInt( population -> population.descriptor.getPropertyKeyId() ).toArray();
    }

    private int[] labelIds()
    {
        return populations.stream().mapToInt( population -> population.descriptor.getLabelId() ).toArray();
    }

    public void cancel()
    {
        replaceIndexCounts( 0, 0, 0 );
        close( false );
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

    private void populateFromQueueIfAvailable( long currentlyIndexedNodeId )
    {
        if ( !queue.isEmpty() )
        {
            try ( MultipleIndexUpdater updater = newPopulatingUpdater( storeView ) )
            {
                do
                {
                    // no need to check for null as nobody else is emptying this queue
                    NodePropertyUpdate update = queue.poll();
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
        private final Map<IndexPopulation,IndexUpdater> populationsWithUpdaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final Log log;

        MultipleIndexUpdater( MultipleIndexPopulator multipleIndexPopulator,
                Map<IndexPopulation,IndexUpdater> populationsWithUpdaters, LogProvider logProvider )
        {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.populationsWithUpdaters = populationsWithUpdaters;
            this.log = logProvider.getLog( getClass() );
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds )
        {
            throw new UnsupportedOperationException( "Index populators don't do removal" );
        }

        @Override
        public void process( NodePropertyUpdate update )
        {
            Iterator<Map.Entry<IndexPopulation,IndexUpdater>> iterator = populationsWithUpdaters.entrySet().iterator();
            while ( iterator.hasNext() )
            {
                Map.Entry<IndexPopulation,IndexUpdater> entry = iterator.next();
                IndexPopulation population = entry.getKey();
                IndexUpdater updater = entry.getValue();

                if ( population.isApplicable( update ) )
                {
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
                        iterator.remove();
                        multipleIndexPopulator.fail( population, t );
                    }
                }
            }
        }

        @Override
        public void close()
        {
            Iterator<Map.Entry<IndexPopulation,IndexUpdater>> iterator = populationsWithUpdaters.entrySet().iterator();
            while ( iterator.hasNext() )
            {
                Map.Entry<IndexPopulation,IndexUpdater> entry = iterator.next();
                IndexPopulation population = entry.getKey();
                IndexUpdater updater = entry.getValue();

                try
                {
                    updater.close();
                }
                catch ( Throwable t )
                {
                    iterator.remove();
                    multipleIndexPopulator.fail( population, t );
                }
            }
        }
    }

    public class IndexPopulation
    {
        public final IndexPopulator populator;
        final IndexDescriptor descriptor;
        final IndexConfiguration config;
        final SchemaIndexProvider.Descriptor providerDescriptor;
        final IndexCountsRemover indexCountsRemover;
        final FlippableIndexProxy flipper;
        final FailedIndexProxyFactory failedIndexProxyFactory;
        final String indexUserDescription;
        private Collection<NodePropertyUpdate> applicableUpdates = new ArrayList<>();

        IndexPopulation(
                IndexPopulator populator,
                IndexDescriptor descriptor,
                IndexConfiguration config,
                Descriptor providerDescriptor,
                FlippableIndexProxy flipper,
                FailedIndexProxyFactory failedIndexProxyFactory,
                String indexUserDescription )
        {
            this.populator = populator;
            this.descriptor = descriptor;
            this.config = config;
            this.providerDescriptor = providerDescriptor;
            this.flipper = flipper;
            this.failedIndexProxyFactory = failedIndexProxyFactory;
            this.indexUserDescription = indexUserDescription;
            this.indexCountsRemover = new IndexCountsRemover( storeView, descriptor );
        }

        private void flipToFailed( Throwable t )
        {
            flipper.flipTo( new FailedIndexProxy( descriptor, config, providerDescriptor, indexUserDescription,
                    populator, failure( t ), indexCountsRemover, logProvider ) );
        }

        private void addAll( Collection<NodePropertyUpdate> updates )
                throws IndexEntryConflictException, IOException
        {
            for ( NodePropertyUpdate update : updates )
            {
                if ( isApplicable( update ) )
                {
                    populator.includeSample( update );
                    applicableUpdates.add( update );
                }
            }
            if ( !applicableUpdates.isEmpty() )
            {
                addApplicable( applicableUpdates );
                applicableUpdates.clear();
            }
        }

        void addApplicable( Collection<NodePropertyUpdate> updates )
                throws IOException, IndexEntryConflictException
        {
            populator.add( updates );
        }

        private boolean isApplicable( NodePropertyUpdate update )
        {
            return update.forLabel( descriptor.getLabelId() ) &&
                   update.getPropertyKeyId() == descriptor.getPropertyKeyId();
        }

        private void flip() throws FlipFailedKernelException
        {
            flipper.flip( () -> {
                populateFromQueueIfAvailable( Long.MAX_VALUE );
                IndexSample sample = populator.sampleResult();
                storeView.replaceIndexCounts( descriptor, sample.uniqueValues(), sample.sampleSize(),
                        sample.indexSize() );
                populator.close( true );
                return null;
            }, failedIndexProxyFactory );
            log.info( "Index population completed. Index is now online: [%s]", indexUserDescription );
        }
    }

    private class NodePopulationVisitor implements Visitor<NodePropertyUpdates,
            IndexPopulationFailedKernelException>
    {
        @Override
        public boolean visit( NodePropertyUpdates updates ) throws IndexPopulationFailedKernelException
        {
            add( updates );
            populateFromQueueBatched( updates.getNodeId() );
            return false;
        }

        private void add( NodePropertyUpdates updates )
        {
            forEachPopulation( population -> population.addAll( updates.getPropertyUpdates() ) );
        }
    }
}
