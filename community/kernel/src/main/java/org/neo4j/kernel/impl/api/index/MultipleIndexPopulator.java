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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLong.Out;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static java.lang.String.format;

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
    // Concurrency queue since multiple concurrent threads may enqueue updates into it
    private final Queue<NodePropertyUpdate> queue = new ConcurrentLinkedQueue<>();

    // Populators are added into this list. The same thread adding populators will later call #indexAllNodes
    // So doesn't need to be synchronized
    private final List<IndexPopulation> populations = new LinkedList<>();

    private final IndexStoreView storeView;
    private final LogProvider logProvider;
    private final Log log;

    public MultipleIndexPopulator( IndexStoreView storeView, LogProvider logProvider )
    {
        this.storeView = storeView;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( IndexPopulationJob.class );
    }

    public void addPopulator(
            IndexPopulator populator,
            IndexDescriptor descriptor,
            Descriptor providerDescriptor, IndexConfiguration config,
            FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory,
            String indexUserDescription )
    {
        populations.add( new IndexPopulation( populator, descriptor, config, providerDescriptor,
                flipper, failedIndexProxyFactory, indexUserDescription ) );
    }

    public boolean hasPopulators()
    {
        return !populations.isEmpty();
    }

    @Override
    public void create()
    {
        for ( int i = 0; i < populations.size(); )
        {
            IndexPopulation population = populations.get( i );
            log.info( "Index population started: [%s]", population.getIndexUserDescription() );
            try
            {
                population.populator.create();
                i++; // only increment here on success since we remove from list on failure
            }
            catch ( Throwable t )
            {
                fail( i, t );
            }
        }
    }

    @Override
    public void drop() throws IOException
    {
        throw new UnsupportedOperationException( "Can't drop indexes from this populator implementation" );
    }

    @Override
    public void add( long nodeId, Object propertyValue )
    {
        throw new UnsupportedOperationException( "Can't populate directly using this populator implementation. " );
    }

    public StoreScan<IndexPopulationFailedKernelException> indexAllNodes()
    {
        return storeView.visitNodes( labelIds(), propertyKeyIds(), new NodePopulationVisitor() );
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
    public void fail( Throwable t )
    {
        while ( !populations.isEmpty() )
        {
            fail( 0, t );
        }
    }

    private void fail( int index, Throwable t )
    {
        IndexPopulation population = populations.remove( index );

        // If the cause of index population failure is a conflict in a (unique) index, the conflict is the
        // failure
        // TODO do we need this?
        if ( t instanceof IndexPopulationFailedKernelException )
        {
            Throwable cause = t.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                t = cause;
            }
        }

        // Index conflicts are expected (for unique indexes) so we don't need to log them.
        if ( !(t instanceof IndexEntryConflictException) /*TODO: && this is a unique index...*/ )
        {
            log.error( format( "Failed to populate index: [%s]", population.getIndexUserDescription() ), t );
        }

        // The flipper will have already flipped to a failed index context here, but
        // it will not include the cause of failure, so we do another flip to a failed
        // context that does.

        // The reason for having the flipper transition to the failed index context in the first
        // place is that we would otherwise introduce a race condition where updates could come
        // in to the old context, if something failed in the job we send to the flipper.
        population.flipToFailed( t );
        try
        {
            population.populator.markAsFailed( failure( t ).asString() );
            population.populator.close( false );
        }
        catch ( Throwable e )
        {
            log.error( format( "Unable to close failed populator for index: [%s]",
                    population.getIndexUserDescription() ), e );
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        throw new UnsupportedOperationException( "Should not be called directly" );
    }

    public void verifyAllDeferredConstraints( PropertyAccessor accessor )
    {
        for ( int i = 0; i < populations.size(); )
        {
            IndexPopulation population = populations.get( i );
            try
            {
                population.populator.verifyDeferredConstraints( accessor );
                i++; // only increment here on success since we remove from list on failure
            }
            catch ( Throwable t )
            {
                fail( i, t );
            }
        }
    }

    @Override
    public MultipleIndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        List<IndexUpdater> updaters = new ArrayList<>();
        for ( int i = 0; i < populations.size(); )
        {
            IndexPopulation population = populations.get( i );
            try
            {
                updaters.add( population.populator.newPopulatingUpdater( accessor ) );
                i++; // only increment here on success since we remove from list on failure
            }
            catch ( Throwable t )
            {
                fail( i, t );
                // It's OK, just don't include this updater, right?
            }
        }
        return new MultipleIndexUpdater( this, updaters, logProvider );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        for ( int i = 0; i < populations.size(); )
        {
            IndexPopulation population = populations.get( i );
            try
            {
                population.populator.close( populationCompletedSuccessfully );
                i++; // only increment here on success since we remove from list on failure
            }
            catch ( Throwable t )
            {
                fail( i, t );
            }
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        throw new UnsupportedOperationException( "Multiple index populator can't be marked as failed." );
    }

    @Override
    public long sampleResult( Out result )
    {
        throw new UnsupportedOperationException( "Multiple index populator can't perform index sampling." );
    }

    public void replaceIndexCounts( long uniqueElements, long maxUniqueElements, long indexSize )
    {
        populations.forEach( population ->
                storeView.replaceIndexCounts( population.descriptor, uniqueElements,
                        maxUniqueElements, indexSize ) );
    }

    public void flipAfterPopulation()
    {
        for ( int i = 0; i < populations.size(); )
        {
            IndexPopulation populator = populations.get( i );
            try
            {
                populator.flip();
                i++; // only increment here on success since we remove from list on failure
            }
            catch ( Throwable t )
            {
                fail( i, t );
            }
        }
    }

    private int[] propertyKeyIds()
    {
        int[] ids = new int[populations.size()];
        for ( int i = 0; i < populations.size(); i++ )
        {
            ids[i] = populations.get( i ).descriptor.getPropertyKeyId();
        }
        return ids;
    }

    private int[] labelIds()
    {
        int[] ids = new int[populations.size()];
        for ( int i = 0; i < populations.size(); i++ )
        {
            ids[i] = populations.get( i ).descriptor.getLabelId();
        }
        return ids;
    }

    public void cancel()
    {
        replaceIndexCounts( 0, 0, 0 );
        close( false );
    }

    private void populateFromQueueIfAvailable( final long currentlyIndexedNodeId )
    {
        if ( !queue.isEmpty() )
        {
            try ( MultipleIndexUpdater updater = newPopulatingUpdater( storeView ) )
            {
                do
                {
                    // no need to check for null as nobody else is emptying this queue
                    NodePropertyUpdate update = queue.poll();
                    // TODO: We see updates twice here from IndexStatisticsTest
                    if ( update.getNodeId() <= currentlyIndexedNodeId )
                    {
                        updater.process( update );
                    }
                }
                while ( !queue.isEmpty() );
            }
        }
    }

    private static class MultipleIndexUpdater implements IndexUpdater
    {
        private final List<IndexUpdater> updaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final Log log;

        MultipleIndexUpdater( MultipleIndexPopulator multipleIndexPopulator, List<IndexUpdater> updaters,
                LogProvider logProvider )
        {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.updaters = updaters;
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
            for ( int i = 0; i < updaters.size(); )
            {
                IndexPopulation populator = multipleIndexPopulator.getPopulator(i);
                if ( populator.isApplicable( update ) )
                {
                    IndexUpdater updater = updaters.get( i );
                    try
                    {
                        updater.process( update );
                        i++; // only increment here on success since we remove from list on failure
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
                        updaters.remove( i );
                        multipleIndexPopulator.fail( i, t );
                    }
                }
                else
                {
                    i++; // only increment here on success (ignored) since we remove from list on failure
                }
            }
        }

        @Override
        public void close()
        {
            for ( int i = 0; i < updaters.size(); )
            {
                IndexUpdater updater = updaters.get( i );
                try
                {
                    updater.close();
                    i++; // only increment here on success since we remove from list on failure
                }
                catch ( Throwable t )
                {
                    updaters.remove( i );
                    multipleIndexPopulator.fail( i, t );
                }
            }
        }
    }

    private IndexPopulation getPopulator( int index )
    {
        return populations.get( index );
    }

    private class IndexPopulation
    {
        final IndexPopulator populator;
        final IndexDescriptor descriptor;
        final IndexConfiguration config;
        final SchemaIndexProvider.Descriptor providerDescriptor;
        final IndexCountsRemover indexCountsRemover;
        final FlippableIndexProxy flipper;
        final FailedIndexProxyFactory failedIndexProxyFactory;
        final String indexUserDescription;

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

        public void flipToFailed( Throwable t )
        {
            flipper.flipTo( new FailedIndexProxy( descriptor, config, providerDescriptor, indexUserDescription,
                    populator, failure( t ), indexCountsRemover, logProvider ) );
        }

        public void add( NodePropertyUpdate update )
                throws IndexEntryConflictException, IOException
        {
            if ( isApplicable( update ) )
            {
                populator.add( update.getNodeId(), update.getValueAfter() );
            }
        }

        boolean isApplicable( NodePropertyUpdate update )
        {
            return update.forLabel( descriptor.getLabelId() ) &&
                   update.getPropertyKeyId() == descriptor.getPropertyKeyId();
        }

        void flip() throws FlipFailedKernelException
        {
            flipper.flip( () -> {
                populateFromQueueIfAvailable( Long.MAX_VALUE );
                DoubleLongRegister result = Registers.newDoubleLongRegister();
                long indexSize = populator.sampleResult( result );
                storeView.replaceIndexCounts( descriptor, result.readFirst(), result.readSecond(),
                        indexSize );

                populator.close( true );
                return null;
            }, failedIndexProxyFactory );
            log.info( "Index population completed. Index is now online: [%s]", indexUserDescription );
        }

        public String getIndexUserDescription()
        {
            return indexUserDescription;
        }
    }

    private class NodePopulationVisitor implements Visitor<NodePropertyUpdate,IndexPopulationFailedKernelException>
    {
        @Override
        public boolean visit( NodePropertyUpdate update ) throws IndexPopulationFailedKernelException
        {
            add( update );
            populateFromQueueIfAvailable( update.getNodeId() );
            return false;
        }

        private void add( NodePropertyUpdate update )
        {
            for ( int i = 0; i < populations.size(); )
            {
                IndexPopulation populator = populations.get( i );
                try
                {
                    populator.add( update );
                    i++; // only increment here on success since we remove from list on failure
                }
                catch ( Throwable t )
                {
                    fail( i, t );
                }
            }
        }
    }
}
