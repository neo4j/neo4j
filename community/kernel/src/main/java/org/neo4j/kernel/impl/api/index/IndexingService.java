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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.DirectIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.indexPopulation;

/**
 * Manages the indexes that were introduced in 2.0. These indexes depend on the normal neo4j logical log for
 * transactionality. Each index has an {@link org.neo4j.kernel.impl.store.record.IndexRule}, which it uses to filter
 * changes that come into the database. Changes that apply to the the rule are indexed. This way, "normal" changes to
 * the database can be replayed to perform recovery after a crash.
 * <p>
 * <h3>Recovery procedure</h3>
 * <p>
 * Each index has a state, as defined in {@link org.neo4j.kernel.api.index.InternalIndexState}, which is used during
 * recovery. If an index is anything but {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, it will simply be
 * destroyed and re-created.
 * <p>
 * If, however, it is {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, the index provider is required to
 * also guarantee that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter
{
    private final IndexSamplingController samplingController;
    private final IndexProxyCreator indexProxyCreator;
    private final IndexStoreView storeView;
    private final SchemaIndexProviderMap providerMap;
    private final IndexMapReference indexMapRef;
    private final Iterable<IndexRule> indexRules;
    private final Log log;
    private final TokenNameLookup tokenNameLookup;
    private final MultiPopulatorFactory multiPopulatorFactory;
    private final LogProvider logProvider;
    private final Monitor monitor;
    private final PrimitiveLongSet recoveredNodeIds = Primitive.longSet( 20 );
    private final JobScheduler scheduler;
    private final Runnable schemaStateChangeCallback;

    enum State
    {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    public interface Monitor
    {
        void applyingRecoveredData( PrimitiveLongSet recoveredNodeIds );

        void appliedRecoveredData( Iterable<NodePropertyUpdate> updates );

        void populationCompleteOn( IndexDescriptor descriptor );

        void verifyDeferredConstraints();

        void awaitingPopulationOfRecoveredIndex( long indexId, IndexDescriptor descriptor );
    }

    public static class MonitorAdapter implements Monitor
    {
        @Override
        public void appliedRecoveredData( Iterable<NodePropertyUpdate> updates )
        {   // Do nothing
        }

        @Override
        public void applyingRecoveredData( PrimitiveLongSet recoveredNodeIds )
        {   // Do nothing
        }

        @Override
        public void populationCompleteOn( IndexDescriptor descriptor )
        {   // Do nothing
        }

        @Override
        public void verifyDeferredConstraints()
        {   // Do nothing
        }

        @Override
        public void awaitingPopulationOfRecoveredIndex( long indexId, IndexDescriptor descriptor )
        {   // Do nothing
        }
    }

    public static final Monitor NO_MONITOR = new MonitorAdapter();

    private volatile State state = State.NOT_STARTED;

    IndexingService( IndexProxyCreator indexProxyCreator,
            SchemaIndexProviderMap providerMap,
            IndexMapReference indexMapRef,
            IndexStoreView storeView,
            Iterable<IndexRule> indexRules,
            IndexSamplingController samplingController,
            TokenNameLookup tokenNameLookup,
            JobScheduler scheduler,
            Runnable schemaStateChangeCallback,
            MultiPopulatorFactory multiPopulatorFactory,
            LogProvider logProvider,
            Monitor monitor )
    {
        this.indexProxyCreator = indexProxyCreator;
        this.providerMap = providerMap;
        this.indexMapRef = indexMapRef;
        this.storeView = storeView;
        this.indexRules = indexRules;
        this.samplingController = samplingController;
        this.tokenNameLookup = tokenNameLookup;
        this.scheduler = scheduler;
        this.schemaStateChangeCallback = schemaStateChangeCallback;
        this.multiPopulatorFactory = multiPopulatorFactory;
        this.logProvider = logProvider;
        this.monitor = monitor;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Called while the database starts up, before recovery.
     */
    @Override
    public void init()
    {
        IndexMap indexMap = indexMapRef.indexMapSnapshot();

        for ( IndexRule indexRule : indexRules )
        {
            IndexProxy indexProxy;

            long indexId = indexRule.getId();
            IndexDescriptor descriptor = new IndexDescriptor( indexRule.getLabel(), indexRule.getPropertyKey() );
            SchemaIndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
            SchemaIndexProvider provider = providerMap.apply( providerDescriptor );
            InternalIndexState initialState = provider.getInitialState( indexId );
            log.info( indexStateInfo( "init", indexId, initialState, descriptor ) );
            boolean constraint = indexRule.isConstraintIndex();

            switch ( initialState )
            {
                case ONLINE:
                    indexProxy =
                        indexProxyCreator.createOnlineIndexProxy( indexId, descriptor, providerDescriptor, constraint );
                    break;
                case POPULATING:
                    // The database was shut down during population, or a crash has occurred, or some other sad thing.
                    if ( constraint && indexRule.getOwningConstraint() == null )
                    {
                        // don't bother rebuilding if we are going to throw the index away anyhow
                        indexProxy = indexProxyCreator.createFailedIndexProxy(
                                indexId, descriptor, providerDescriptor, constraint,
                                failure( "Constraint for index was not committed." ) );
                    }
                    else
                    {
                        indexProxy = indexProxyCreator
                                .createRecoveringIndexProxy( descriptor, providerDescriptor, constraint );
                    }
                    break;
                case FAILED:
                    IndexPopulationFailure failure = failure( provider.getPopulationFailure( indexId ) );
                    indexProxy = indexProxyCreator
                            .createFailedIndexProxy( indexId, descriptor, providerDescriptor, constraint, failure );
                    break;
                default:
                    throw new IllegalArgumentException( "" + initialState );
            }
            indexMap.putIndexProxy( indexId, indexProxy );
        }

        indexMapRef.setIndexMap( indexMap );
    }

    // Recovery semantics: This is to be called after init, and after the database has run recovery.
    @Override
    public void start() throws Exception
    {
        state = State.STARTING;

        applyRecoveredUpdates();
        IndexMap indexMap = indexMapRef.indexMapSnapshot();

        final Map<Long,RebuildingIndexDescriptor> rebuildingDescriptors = new HashMap<>();

        // Find all indexes that are not already online, do not require rebuilding, and create them
        indexMap.foreachIndexProxy( ( indexId, proxy ) -> {
            InternalIndexState state = proxy.getState();
            IndexDescriptor descriptor = proxy.getDescriptor();
            log.info( indexStateInfo( "start", indexId, state, descriptor ) );
            switch ( state )
            {
                case ONLINE:
                    // Don't do anything, index is ok.
                    break;
                case POPULATING:
                    // Remember for rebuilding
                    rebuildingDescriptors.put( indexId, new RebuildingIndexDescriptor(
                            descriptor, proxy.getProviderDescriptor(), proxy.config() ) );
                    break;
                case FAILED:
                    // Don't do anything, the user needs to drop the index and re-create
                    break;
                default:
                    throw new IllegalStateException( "Unknown state: " + state );
            }
        } );

        // Drop placeholder proxies for indexes that need to be rebuilt
        dropRecoveringIndexes( indexMap, rebuildingDescriptors.keySet() );

        // Rebuild indexes by recreating and repopulating them
        if ( !rebuildingDescriptors.isEmpty() )
        {
            IndexPopulationJob populationJob = newIndexPopulationJob();
            for ( Map.Entry<Long,RebuildingIndexDescriptor> entry : rebuildingDescriptors.entrySet() )
            {
                long indexId = entry.getKey();
                RebuildingIndexDescriptor descriptor = entry.getValue();

                IndexProxy proxy = indexProxyCreator.createPopulatingIndexProxy(
                        indexId,
                        descriptor.getIndexDescriptor(),
                        descriptor.getProviderDescriptor(),
                        descriptor.getConfiguration(),
                        false, // never pass through a tentative online state during recovery
                        monitor,
                        populationJob );
                proxy.start();
                indexMap.putIndexProxy( indexId, proxy );
            }
            startIndexPopulation( populationJob );
        }

        indexMapRef.setIndexMap( indexMap );

        samplingController.recoverIndexSamples();
        samplingController.start();

        // So at this point we've started population of indexes that needs to be rebuilt in the background.
        // Indexes backing uniqueness constraints are normally built within the transaction creating the constraint
        // and so we shouldn't leave such indexes in a populating state after recovery.
        // This is why we now go and wait for those indexes to be fully populated.
        for ( Map.Entry<Long,RebuildingIndexDescriptor> entry : rebuildingDescriptors.entrySet() )
        {
            if ( !entry.getValue().getConfiguration().isUnique() )
            {
                // It's not a uniqueness constraint, so don't wait for it to be rebuilt
                continue;
            }

            IndexProxy proxy;
            try
            {
                proxy = getIndexProxy( entry.getKey() );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new IllegalStateException(
                        "What? This index was seen during recovery just now, why isn't it available now?" );
            }

            monitor.awaitingPopulationOfRecoveredIndex( entry.getKey(), entry.getValue().getIndexDescriptor() );
            awaitOnline( proxy );
        }

        state = State.RUNNING;
    }

    /**
     * Polls the {@link IndexProxy#getState() state of the index} and waits for it to be either
     * {@link InternalIndexState#ONLINE}, in which case the wait is over, or {@link InternalIndexState#FAILED},
     * in which an exception is thrown.
     */
    private void awaitOnline( IndexProxy proxy ) throws InterruptedException
    {
        while ( true )
        {
            switch ( proxy.getState() )
            {
            case ONLINE:
                return;
            case FAILED:
                throw new IllegalStateException(
                        "Index entered " + FAILED + " state while recovery waited for it to be fully populated" );
            case POPULATING:
                // Sleep a short while and look at state again the next loop iteration
                Thread.sleep( 10 );
                break;
            default:
                throw new IllegalStateException( proxy.getState().name() );
            }
        }
    }

    // We need to stop indexing service on shutdown since we can have transactions that are ongoing/finishing
    // after we start stopping components and those transactions should be able to finish successfully
    @Override
    public void shutdown()
    {
        state = State.STOPPED;
        samplingController.stop();
        closeAllIndexes();
    }

    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        final IndexProxy indexProxy = indexMapRef.getOnlineIndexProxy( descriptor );
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        storeView.indexUpdatesAndSize( indexProxy.getDescriptor(), output );
        return output;
    }

    public double indexUniqueValuesPercentage( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        final IndexProxy indexProxy = indexMapRef.getOnlineIndexProxy( descriptor );
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        storeView.indexSample( indexProxy.getDescriptor(), output );
        long unique = output.readFirst();
        long size = output.readSecond();
        if ( size == 0 )
        {
            return 1.0d;
        }
        else
        {
            return ((double) unique) / ((double) size);
        }
    }

    /**
     * Applies updates from the given {@link IndexUpdates}, which may contain updates for one or more indexes.
     * As long as index updates are derived from physical commands and store state there's special treatment
     * during recovery since we cannot read from an unrecovered store, so in that case the nodes ids are simply
     * noted and reindexed after recovery of the store has completed. That is also why {@link IndexUpdates}
     * has one additional accessor method for getting the node ids.
     *
     * As far as {@link IndexingService} is concerned recovery happens between calls to {@link #init()} and
     * {@link #start()}.
     *
     * @param updates {@link IndexUpdates} to apply.
     * @throws IOException potentially thrown from index updating.
     * @throws IndexEntryConflictException potentially thrown from index updating.
     */
    public void apply( IndexUpdates updates ) throws IOException, IndexEntryConflictException
    {
        if ( state == State.NOT_STARTED )
        {
            // We're in recovery, which means we'll merely be noting which entity ids are to be refreshed
            // and we'll refresh them completely after recovery completes.
            updates.collectUpdatedNodeIds( recoveredNodeIds );
        }
        else if ( state == State.RUNNING || state == State.STARTING )
        {
            apply( updates, IndexUpdateMode.ONLINE );
        }
        else
        {
            throw new IllegalStateException(
                    "Can't apply index updates " + asList( updates ) + " while indexing service is " + state );
        }
    }

    private void apply( IndexUpdates updates, IndexUpdateMode updateMode )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( updateMode ) )
        {
            // TODO Sort the updates by index so that we don't have to create an IndexDescriptor object per update
            for ( NodePropertyUpdate update : updates )
            {
                int propertyKeyId = update.getPropertyKeyId();
                switch ( update.getUpdateMode() )
                {
                case ADDED:
                    for ( int len = update.getNumberOfLabelsAfter(), i = 0; i < len; i++ )
                    {
                        processUpdate( update, updaterMap, update.getLabelAfter( i ), propertyKeyId );
                    }
                    break;

                case REMOVED:
                    for ( int len = update.getNumberOfLabelsBefore(), i = 0; i < len; i++ )
                    {
                        processUpdate( update, updaterMap, update.getLabelBefore( i ), propertyKeyId );
                    }
                    break;

                case CHANGED:
                    int lenBefore = update.getNumberOfLabelsBefore();
                    int lenAfter = update.getNumberOfLabelsAfter();

                    for ( int i = 0, j = 0; i < lenBefore && j < lenAfter; )
                    {
                        int labelBefore = update.getLabelBefore( i );
                        int labelAfter = update.getLabelAfter( j );

                        if ( labelBefore == labelAfter )
                        {
                            processUpdate( update, updaterMap, labelAfter, propertyKeyId );
                            i++;
                            j++;
                        }
                        else
                        {
                            if ( labelBefore < labelAfter )
                            {
                                i++;
                            }
                            else /* labelBefore > labelAfter */
                            {
                                j++;
                            }
                        }
                    }
                    break;

                default:
                    throw new IllegalStateException( "Unknown update mode: " + update.getUpdateMode() );
                }
            }
        }
    }

    private void processUpdate( NodePropertyUpdate update, IndexUpdaterMap updaterMap, int labelId,
            int propertyKeyId ) throws IOException, IndexEntryConflictException
    {
        IndexDescriptor descriptor = new IndexDescriptor( labelId, propertyKeyId );
        IndexUpdater updater = updaterMap.getUpdater( descriptor );
        if ( updater != null )
        {
            updater.process( update );
        }
    }

    /**
     * Creates one or more indexes. They will all be populated by one and the same store scan.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     */
    public void createIndexes( IndexRule... rules ) throws IOException
    {
        IndexMap indexMap = indexMapRef.indexMapSnapshot();
        IndexPopulationJob populationJob = null;

        for ( IndexRule rule : rules )
        {
            long ruleId = rule.getId();
            IndexProxy index = indexMap.getIndexProxy( ruleId );
            if ( index != null && state == State.NOT_STARTED )
            {
                // During recovery we might run into this scenario:
                // - We're starting recovery on a database, where init() is called and all indexes that
                //   are found in the store, instantiated and put into the IndexMap. Among them is index X.
                // - While we recover the database we bump into a transaction creating index Y, with the
                //   same IndexDescriptor, i.e. same label/property, as X. This is possible since this took
                //   place before the creation of X.
                // - When Y is dropped in between this creation and the creation of X (it will have to be
                //   otherwise X wouldn't have had an opportunity to be created) the index is removed from
                //   the IndexMap, both by id AND descriptor.
                //
                // Because of the scenario above we need to put this created index into the IndexMap
                // again, otherwise it will disappear from the IndexMap (at least for lookup by descriptor)
                // and not be able to accept changes applied from recovery later on.
                indexMap.putIndexProxy( ruleId, index );
                indexMapRef.setIndexMap( indexMap );
                continue;
            }
            final IndexDescriptor descriptor = new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
            SchemaIndexProvider.Descriptor providerDescriptor = rule.getProviderDescriptor();
            boolean constraint = rule.isConstraintIndex();
            if ( state == State.RUNNING )
            {
                populationJob = populationJob == null ? newIndexPopulationJob() : populationJob;
                index = indexProxyCreator.createPopulatingIndexProxy(
                        ruleId, descriptor, providerDescriptor, IndexConfiguration.of( constraint ), constraint,
                        monitor, populationJob );
                index.start();
            }
            else
            {
                index = indexProxyCreator.createRecoveringIndexProxy( descriptor, providerDescriptor, constraint );
            }

            indexMap.putIndexProxy( rule.getId(), index );
        }

        if ( populationJob != null )
        {
            startIndexPopulation( populationJob );
        }
        indexMapRef.setIndexMap( indexMap );
    }

    private void applyRecoveredUpdates() throws IOException, IndexEntryConflictException
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Applying recovered updates: " + recoveredNodeIds );
        }
        monitor.applyingRecoveredData( recoveredNodeIds );
        if ( !recoveredNodeIds.isEmpty() )
        {
            try ( IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( IndexUpdateMode.RECOVERY ) )
            {
                for ( IndexUpdater updater : updaterMap )
                {
                    updater.remove( recoveredNodeIds );
                }

                IndexUpdates updates = readRecoveredUpdatesFromStore();
                apply( updates, IndexUpdateMode.RECOVERY );
                monitor.appliedRecoveredData( updates );
            }
        }
        recoveredNodeIds.clear();
    }

    private IndexUpdates readRecoveredUpdatesFromStore()
    {
        final List<NodePropertyUpdate> recoveredUpdates = new ArrayList<>();
        recoveredNodeIds.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long nodeId )
            {
                storeView.nodeAsUpdates( nodeId, recoveredUpdates );
                return false;
            }
        } );

        return new DirectIndexUpdates( recoveredUpdates );
    }

    public void dropIndex( IndexRule rule )
    {
        long indexId = rule.getId();
        IndexProxy index = indexMapRef.removeIndexProxy( indexId );
        if ( state == State.RUNNING )
        {
            assert index != null : "Index " + rule + " doesn't exists";
            try
            {
                Future<Void> dropFuture = index.drop();
                awaitIndexFuture( dropFuture );
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
    }

    public void triggerIndexSampling( IndexSamplingMode mode )
    {
        log.info( "Manual trigger for sampling all indexes [" + mode + "]" );
        samplingController.sampleIndexes( mode );
    }

    public void triggerIndexSampling( IndexDescriptor descriptor, IndexSamplingMode mode )
    {
        String description = descriptor.userDescription( tokenNameLookup );
        log.info( "Manual trigger for sampling index " + description + " [" + mode + "]" );
        samplingController.sampleIndex( descriptor, mode );
    }

    private void awaitIndexFuture( Future<Void> future ) throws Exception
    {
        try
        {
            future.get( 1, MINUTES );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw e;
        }
    }

    private void dropRecoveringIndexes( IndexMap indexMap, Iterable<Long> indexesToRebuild )
            throws IOException
    {
        for ( long indexId : indexesToRebuild )
        {
            IndexProxy indexProxy = indexMap.removeIndexProxy( indexId );
            indexProxy.drop();
        }
    }

    public void activateIndex( long indexId ) throws
            IndexNotFoundKernelException, IndexActivationFailedKernelException, IndexPopulationFailedKernelException
    {
        try
        {
            if ( state == State.RUNNING ) // don't do this during recovery.
            {
                IndexProxy index = getIndexProxy( indexId );
                index.awaitStoreScanCompleted();
                index.activate();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new IndexActivationFailedKernelException( e, "Unable to activate index, thread was interrupted." );
        }
    }

    public IndexProxy getIndexProxy( long indexId ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexProxy( indexId );
    }

    public IndexProxy getIndexProxy( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexProxy( descriptor );
    }

    public void validateIndex( long indexId ) throws IndexNotFoundKernelException, ConstraintVerificationFailedKernelException, IndexPopulationFailedKernelException
    {
        getIndexProxy( indexId ).validate();
    }

    public void forceAll()
    {
        for ( IndexProxy index : indexMapRef.getAllIndexProxies() )
        {
            try
            {
                index.force();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to force " + index, e );
            }
        }
    }

    public void flushAll()
    {
        for ( IndexProxy index : indexMapRef.getAllIndexProxies() )
        {
            try
            {
                index.flush();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to force " + index, e );
            }
        }
    }

    private void closeAllIndexes()
    {
        Iterable<IndexProxy> indexesToStop = indexMapRef.clear();
        Collection<Future<Void>> indexStopFutures = new ArrayList<>();
        for ( IndexProxy index : indexesToStop )
        {
            try
            {
                indexStopFutures.add( index.close() );
            }
            catch ( IOException e )
            {
                log.error( "Unable to close index", e );
            }
        }

        for ( Future<Void> future : indexStopFutures )
        {
            try
            {
                awaitIndexFuture( future );
            }
            catch ( Exception e )
            {
                log.error( "Error awaiting index to close", e );
            }
        }
    }

    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        Set<SchemaIndexProvider.Descriptor> fromProviders = new HashSet<>();
        for ( IndexProxy indexProxy : indexMapRef.getAllIndexProxies() )
        {
            Descriptor providerDescriptor = indexProxy.getProviderDescriptor();
            if ( fromProviders.add( providerDescriptor ) )
            {
                snapshots.add( providerMap.apply( providerDescriptor ).snapshotMetaFiles() );
            }
            snapshots.add( indexProxy.snapshotFiles() );
        }

        return Iterators.concatResourceIterators( snapshots.iterator() );
    }

    private IndexPopulationJob newIndexPopulationJob()
    {
        MultipleIndexPopulator multiPopulator = multiPopulatorFactory.create( storeView, logProvider );
        return new IndexPopulationJob( storeView, multiPopulator, monitor, schemaStateChangeCallback );
    }

    private void startIndexPopulation( IndexPopulationJob job )
    {
        scheduler.schedule( indexPopulation, job );
    }

    private String indexStateInfo( String tag, Long indexId, InternalIndexState state, IndexDescriptor descriptor )
    {
        return format( "IndexingService.%s: index %d on %s is %s", tag, indexId,
                descriptor.userDescription( tokenNameLookup ), state.name() );
    }

    public IndexSamplingController getSamplingController()
    {
        return samplingController;
    }
}
