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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * Manages the indexes that were introduced in 2.0. These indexes depend on the normal neo4j logical log for
 * transactionality. Each index has an {@link StoreIndexDescriptor}, which it uses to filter
 * changes that come into the database. Changes that apply to the the rule are indexed. This way, "normal" changes to
 * the database can be replayed to perform recovery after a crash.
 * <p>
 * <h3>Recovery procedure</h3>
 * <p>
 * Each index has a state, as defined in {@link InternalIndexState}, which is used during
 * recovery. If an index is anything but {@link InternalIndexState#ONLINE}, it will simply be
 * destroyed and re-created.
 * <p>
 * If, however, it is {@link InternalIndexState#ONLINE}, the index provider is required to
 * also guarantee that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter implements IndexingUpdateService, IndexingProvidersService
{
    private final IndexSamplingController samplingController;
    private final IndexProxyCreator indexProxyCreator;
    private final IndexStoreView storeView;
    private final IndexProviderMap providerMap;
    private final IndexMapReference indexMapRef;
    private final Iterable<StoreIndexDescriptor> indexDescriptors;
    private final Log internalLog;
    private final Log userLog;
    private final TokenNameLookup tokenNameLookup;
    private final MultiPopulatorFactory multiPopulatorFactory;
    private final LogProvider internalLogProvider;
    private final Monitor monitor;
    private final SchemaState schemaState;
    private final IndexPopulationJobController populationJobController;
    private final Map<Long,IndexProxy> indexesToDropAfterCompletedRecovery = new HashMap<>();

    enum State
    {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    public interface Monitor
    {
        void initialState( StoreIndexDescriptor descriptor, InternalIndexState state );

        void populationCompleteOn( StoreIndexDescriptor descriptor );

        void indexPopulationScanStarting();

        void indexPopulationScanComplete();

        void awaitingPopulationOfRecoveredIndex( StoreIndexDescriptor descriptor );
    }

    public static class MonitorAdapter implements Monitor
    {
        @Override
        public void initialState( StoreIndexDescriptor descriptor, InternalIndexState state )
        {   // Do nothing
        }

        @Override
        public void populationCompleteOn( StoreIndexDescriptor descriptor )
        {   // Do nothing
        }

        @Override
        public void indexPopulationScanStarting()
        {   // Do nothing
        }

        @Override
        public void indexPopulationScanComplete()
        {   // Do nothing
        }

        @Override
        public void awaitingPopulationOfRecoveredIndex( StoreIndexDescriptor descriptor )
        {   // Do nothing
        }
    }

    public static final Monitor NO_MONITOR = new MonitorAdapter();

    private volatile State state = State.NOT_STARTED;

    IndexingService( IndexProxyCreator indexProxyCreator,
            IndexProviderMap providerMap,
            IndexMapReference indexMapRef,
            IndexStoreView storeView,
            Iterable<StoreIndexDescriptor> indexDescriptors,
            IndexSamplingController samplingController,
            TokenNameLookup tokenNameLookup,
            JobScheduler scheduler,
            SchemaState schemaState,
            MultiPopulatorFactory multiPopulatorFactory,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            Monitor monitor )
    {
        this.indexProxyCreator = indexProxyCreator;
        this.providerMap = providerMap;
        this.indexMapRef = indexMapRef;
        this.storeView = storeView;
        this.indexDescriptors = indexDescriptors;
        this.samplingController = samplingController;
        this.tokenNameLookup = tokenNameLookup;
        this.schemaState = schemaState;
        this.multiPopulatorFactory = multiPopulatorFactory;
        this.internalLogProvider = internalLogProvider;
        this.monitor = monitor;
        this.populationJobController = new IndexPopulationJobController( scheduler );
        this.internalLog = internalLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    /**
     * Called while the database starts up, before recovery.
     */
    @Override
    public void init()
    {
        validateDefaultProviderExisting();

        indexMapRef.modify( indexMap ->
        {
            Map<InternalIndexState, List<IndexLogRecord>> indexStates = new EnumMap<>( InternalIndexState.class );
            for ( StoreIndexDescriptor indexDescriptor : indexDescriptors )
            {
                IndexProxy indexProxy;

                IndexProviderDescriptor providerDescriptor = indexDescriptor.providerDescriptor();
                IndexProvider provider = providerMap.lookup( providerDescriptor );
                InternalIndexState initialState = provider.getInitialState( indexDescriptor );
                indexStates.computeIfAbsent( initialState, internalIndexState -> new ArrayList<>() )
                        .add( new IndexLogRecord( indexDescriptor ) );

                internalLog.debug( indexStateInfo( "init", initialState, indexDescriptor ) );
                switch ( initialState )
                {
                case ONLINE:
                    monitor.initialState( indexDescriptor, ONLINE );
                    indexProxy = indexProxyCreator.createOnlineIndexProxy( indexDescriptor );
                    break;
                case POPULATING:
                    // The database was shut down during population, or a crash has occurred, or some other sad thing.
                    monitor.initialState( indexDescriptor, POPULATING );
                    indexProxy = indexProxyCreator.createRecoveringIndexProxy( indexDescriptor );
                    break;
                case FAILED:
                    monitor.initialState( indexDescriptor, FAILED );
                    IndexPopulationFailure failure = failure( provider.getPopulationFailure( indexDescriptor ) );
                    indexProxy = indexProxyCreator.createFailedIndexProxy( indexDescriptor, failure );
                    break;
                default:
                    throw new IllegalArgumentException( "" + initialState );
                }
                indexMap.putIndexProxy( indexProxy );
            }
            logIndexStateSummary( "init", indexStates );
            return indexMap;
        } );
    }

    private void validateDefaultProviderExisting()
    {
        if ( providerMap == null || providerMap.getDefaultProvider() == null )
        {
            throw new IllegalStateException( "You cannot run the database without an index provider, " +
                    "please make sure that a valid provider (subclass of " + IndexProvider.class.getName() +
                    ") is on your classpath." );
        }
    }

    // Recovery semantics: This is to be called after init, and after the database has run recovery.
    @Override
    public void start()
    {
        state = State.STARTING;

        // During recovery there could have been dropped indexes. Dropping an index means also updating the counts store,
        // which is problematic during recovery. So instead drop those indexes here, after recovery completed.
        performRecoveredIndexDropActions();

        // Recovery will not do refresh (update read views) while applying recovered transactions and instead
        // do it at one point after recovery... i.e. here
        indexMapRef.indexMapSnapshot().forEachIndexProxy( indexProxyOperation( "refresh", IndexProxy::refresh ) );

        final MutableLongObjectMap<StoreIndexDescriptor> rebuildingDescriptors = new LongObjectHashMap<>();
        indexMapRef.modify( indexMap ->
        {
            Map<InternalIndexState, List<IndexLogRecord>> indexStates = new EnumMap<>( InternalIndexState.class );
            Map<IndexProviderDescriptor,List<IndexLogRecord>> indexProviders = new HashMap<>();

            // Find all indexes that are not already online, do not require rebuilding, and create them
            indexMap.forEachIndexProxy( ( indexId, proxy ) ->
            {
                InternalIndexState state = proxy.getState();
                StoreIndexDescriptor descriptor = proxy.getDescriptor();
                IndexProviderDescriptor providerDescriptor = descriptor.providerDescriptor();
                IndexLogRecord indexLogRecord = new IndexLogRecord( descriptor );
                indexStates.computeIfAbsent( state, internalIndexState -> new ArrayList<>() )
                        .add( indexLogRecord );
                indexProviders.computeIfAbsent( providerDescriptor, indexProviderDescriptor -> new ArrayList<>() )
                        .add( indexLogRecord );
                internalLog.debug( indexStateInfo( "start", state, descriptor ) );
                switch ( state )
                {
                case ONLINE:
                    // Don't do anything, index is ok.
                    break;
                case POPULATING:
                    // Remember for rebuilding
                    rebuildingDescriptors.put( indexId, descriptor );
                    break;
                case FAILED:
                    // Don't do anything, the user needs to drop the index and re-create
                    break;
                default:
                    throw new IllegalStateException( "Unknown state: " + state );
                }
            } );
            logIndexStateSummary( "start", indexStates );
            logIndexProviderSummary( indexProviders );

            // Drop placeholder proxies for indexes that need to be rebuilt
            dropRecoveringIndexes( indexMap, rebuildingDescriptors.keySet() );
            // Rebuild indexes by recreating and repopulating them
            populateIndexesOfAllTypes( rebuildingDescriptors, indexMap );

            return indexMap;
        } );

        samplingController.recoverIndexSamples();
        samplingController.start();

        // So at this point we've started population of indexes that needs to be rebuilt in the background.
        // Indexes backing uniqueness constraints are normally built within the transaction creating the constraint
        // and so we shouldn't leave such indexes in a populating state after recovery.
        // This is why we now go and wait for those indexes to be fully populated.
        rebuildingDescriptors.forEachKeyValue( ( indexId, descriptor ) ->
                {
                    if ( descriptor.type() != IndexDescriptor.Type.UNIQUE )
                    {
                        // It's not a uniqueness constraint, so don't wait for it to be rebuilt
                        return;
                    }

                    IndexProxy proxy;
                    try
                    {
                        proxy = getIndexProxy( indexId );
                    }
                    catch ( IndexNotFoundKernelException e )
                    {
                        throw new IllegalStateException(
                                "What? This index was seen during recovery just now, why isn't it available now?", e );
                    }

                    monitor.awaitingPopulationOfRecoveredIndex( descriptor );
                    awaitOnline( proxy );
                } );

        state = State.RUNNING;
    }

    private void populateIndexesOfAllTypes( MutableLongObjectMap<StoreIndexDescriptor> rebuildingDescriptors, IndexMap indexMap )
    {
        Map<EntityType,MutableLongObjectMap<StoreIndexDescriptor>> rebuildingDescriptorsByType = new EnumMap<>( EntityType.class );
        for ( StoreIndexDescriptor descriptor : rebuildingDescriptors )
        {
            rebuildingDescriptorsByType.computeIfAbsent( descriptor.schema().entityType(), type -> new LongObjectHashMap<>() )
                    .put( descriptor.getId(), descriptor );
        }

        for ( Map.Entry<EntityType,MutableLongObjectMap<StoreIndexDescriptor>> descriptorToPopulate : rebuildingDescriptorsByType.entrySet() )
        {
            IndexPopulationJob populationJob = newIndexPopulationJob( descriptorToPopulate.getKey(), false );
            populate( descriptorToPopulate.getValue(), indexMap, populationJob );
        }
    }

    private void performRecoveredIndexDropActions()
    {
        indexesToDropAfterCompletedRecovery.values().forEach( index ->
        {
            try
            {
                index.drop();
            }
            catch ( Exception e )
            {
                // This is OK to get during recovery because the underlying index can be in any unknown state
                // while we're recovering. Let's just move on to closing it instead.
                try
                {
                    index.close();
                }
                catch ( IOException closeException )
                {
                    // This is OK for the same reason as above
                }
            }
        } );
        indexesToDropAfterCompletedRecovery.clear();
    }

    private void populate( MutableLongObjectMap<StoreIndexDescriptor> rebuildingDescriptors, IndexMap indexMap, IndexPopulationJob populationJob )
    {
        rebuildingDescriptors.forEachKeyValue( ( indexId, descriptor ) ->
        {
            IndexProxy proxy = indexProxyCreator.createPopulatingIndexProxy( descriptor,
         false,// never pass through a tentative online state during recovery
                    monitor, populationJob );
            proxy.start();
            indexMap.putIndexProxy( proxy );
        } );
        startIndexPopulation( populationJob );
    }

    /**
     * Polls the {@link IndexProxy#getState() state of the index} and waits for it to be either
     * {@link InternalIndexState#ONLINE}, in which case the wait is over, or {@link InternalIndexState#FAILED},
     * in which an exception is thrown.
     */
    private void awaitOnline( IndexProxy proxy )
    {
        while ( true )
        {
            switch ( proxy.getState() )
            {
            case ONLINE:
                return;
            case FAILED:
                IndexPopulationFailure populationFailure = proxy.getPopulationFailure();
                String message = String.format( "Index entered %s state while recovery waited for it to be fully populated.", FAILED );
                String causeOfFailure = populationFailure.asString();
                throw new IllegalStateException( IndexPopulationFailure.appendCauseOfFailure( message, causeOfFailure ) );
            case POPULATING:
                // Sleep a short while and look at state again the next loop iteration
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    throw new IllegalStateException( "Waiting for index to become ONLINE was interrupted", e );
                }
                break;
            default:
                throw new IllegalStateException( proxy.getState().name() );
            }
        }
    }

    // We need to stop indexing service on shutdown since we can have transactions that are ongoing/finishing
    // after we start stopping components and those transactions should be able to finish successfully
    @Override
    public void shutdown() throws ExecutionException, InterruptedException
    {
        state = State.STOPPED;
        samplingController.stop();
        populationJobController.stop();
        closeAllIndexes();
    }

    public DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        final long indexId = indexMapRef.getOnlineIndexId( descriptor );
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        storeView.indexUpdatesAndSize( indexId, output );
        return output;
    }

    public double indexUniqueValuesPercentage( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        final long indexId = indexMapRef.getOnlineIndexId( descriptor );
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        storeView.indexSample( indexId, output );
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

    @Override
    public void validateBeforeCommit( SchemaDescriptor index, Value[] tuple )
    {
        indexMapRef.validateBeforeCommit( index, tuple );
    }

    @Override
    public IndexDescriptor getBlessedDescriptorFromProvider( IndexDescriptor index ) throws MisconfiguredIndexException
    {
        IndexProvider provider = providerMap.lookup( index.providerDescriptor() );
        return provider.bless( index );
    }

    @Override
    public IndexProviderDescriptor indexProviderByName( String providerName )
    {
        return providerMap.lookup( providerName ).getProviderDescriptor();
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
     * @throws UncheckedIOException potentially thrown from index updating.
     * @throws IndexEntryConflictException potentially thrown from index updating.
     */
    @Override
    public void apply( IndexUpdates updates ) throws IndexEntryConflictException
    {
        if ( state == State.NOT_STARTED )
        {
            // We're in recovery, which means we'll be telling indexes to apply with additional care for making
            // idempotent changes.
            apply( updates, IndexUpdateMode.RECOVERY );
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

    private void apply( Iterable<IndexEntryUpdate<SchemaDescriptor>> updates, IndexUpdateMode updateMode ) throws IndexEntryConflictException
    {
        try ( IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( updateMode ) )
        {
            for ( IndexEntryUpdate<SchemaDescriptor> indexUpdate : updates )
            {
                processUpdate( updaterMap, indexUpdate );
            }
        }
    }

    @Override
    public Iterable<IndexEntryUpdate<SchemaDescriptor>> convertToIndexUpdates( EntityUpdates entityUpdates, EntityType type )
    {
        Iterable<SchemaDescriptor> relatedIndexes = indexMapRef.getRelatedIndexes(
                                                entityUpdates.entityTokensChanged(),
                                                entityUpdates.entityTokensUnchanged(),
                                                entityUpdates.propertiesChanged(),
                                                type );

        return entityUpdates.forIndexKeys( relatedIndexes, storeView, type );
    }

    /**
     * Creates one or more indexes. They will all be populated by one and the same store scan.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     *
     * {@link IndexPopulator#verifyDeferredConstraints(NodePropertyAccessor)} will not be called as part of populating these indexes,
     * instead that will be done by code that activates the indexes later.
     */
    public void createIndexes( StoreIndexDescriptor... rules )
    {
        createIndexes( false, rules );
    }

    /**
     * Creates one or more indexes. They will all be populated by one and the same store scan.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     *
     * @param verifyBeforeFlipping whether or not to call {@link IndexPopulator#verifyDeferredConstraints(NodePropertyAccessor)}
     * as part of population, before flipping to a successful state.
     */
    public void createIndexes( boolean verifyBeforeFlipping, StoreIndexDescriptor... rules )
    {
        IndexPopulationStarter populationStarter = new IndexPopulationStarter( verifyBeforeFlipping, rules );
        indexMapRef.modify( populationStarter );
        populationStarter.startPopulation();
    }

    private void processUpdate( IndexUpdaterMap updaterMap, IndexEntryUpdate<SchemaDescriptor> indexUpdate ) throws IndexEntryConflictException
    {
        IndexUpdater updater = updaterMap.getUpdater( indexUpdate.indexKey().schema() );
        if ( updater != null )
        {
            updater.process( indexUpdate );
        }
    }

    public void dropIndex( StoreIndexDescriptor rule )
    {
        indexMapRef.modify( indexMap ->
        {
            long indexId = rule.getId();
            IndexProxy index = indexMap.removeIndexProxy( indexId );

            if ( state == State.RUNNING )
            {
                assert index != null : "Index " + rule + " doesn't exists";
                index.drop();
            }
            else if ( index != null )
            {
                // Dropping an index means also updating the counts store, which is problematic during recovery.
                // So instead make a note of it and actually perform the index drops after recovery.
                indexesToDropAfterCompletedRecovery.put( indexId, index );
            }
            return indexMap;
        } );
    }

    public void triggerIndexSampling( IndexSamplingMode mode )
    {
        internalLog.info( "Manual trigger for sampling all indexes [" + mode + "]" );
        samplingController.sampleIndexes( mode );
    }

    public void triggerIndexSampling( SchemaDescriptor descriptor, IndexSamplingMode mode )
            throws IndexNotFoundKernelException
    {
        String description = descriptor.userDescription( tokenNameLookup );
        internalLog.info( "Manual trigger for sampling index " + description + " [" + mode + "]" );
        samplingController.sampleIndex( indexMapRef.getIndexId( descriptor ), mode );
    }

    private void dropRecoveringIndexes( IndexMap indexMap, LongIterable indexesToRebuild )
    {
        indexesToRebuild.forEach( idx ->
        {
            IndexProxy indexProxy = indexMap.removeIndexProxy( idx );
            assert indexProxy != null;
            indexProxy.drop();
        } );
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
                internalLog.info( "Constraint %s is %s.", index.getDescriptor(), ONLINE.name() );
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

    public IndexProxy getIndexProxy( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexProxy( descriptor );
    }

    public long getIndexId( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexId( descriptor );
    }

    public void validateIndex( long indexId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException,
            UniquePropertyValueValidationException
    {
        getIndexProxy( indexId ).validate();
    }

    public void forceAll( IOLimiter limiter )
    {
        indexMapRef.indexMapSnapshot().forEachIndexProxy( indexProxyOperation( "force", proxy -> proxy.force( limiter ) ) );
    }

    private LongObjectProcedure<IndexProxy> indexProxyOperation( String name, ThrowingConsumer<IndexProxy, Exception> operation )
    {
        return ( id, indexProxy ) ->
        {
            try
            {
                operation.accept( indexProxy );
            }
            catch ( Exception e )
            {
                try
                {
                    IndexProxy proxy = indexMapRef.getIndexProxy( id );
                    throw new UnderlyingStorageException( "Unable to " + name + " " + proxy, e );
                }
                catch ( IndexNotFoundKernelException infe )
                {
                    // index was dropped while trying to operate on it, we can continue to other indexes
                }
            }
        };
    }

    private void closeAllIndexes()
    {
        indexMapRef.modify( indexMap ->
        {
            Iterable<IndexProxy> indexesToStop = indexMap.getAllIndexProxies();
            for ( IndexProxy index : indexesToStop )
            {
                try
                {
                    index.close();
                }
                catch ( Exception e )
                {
                    internalLog.error( "Unable to close index", e );
                }
            }
            // Effectively clearing it
            return new IndexMap();
        } );
    }

    public LongSet getIndexIds()
    {
        Iterable<IndexProxy> indexProxies = indexMapRef.getAllIndexProxies();
        MutableLongSet indexIds = new LongHashSet();
        for ( IndexProxy indexProxy : indexProxies )
        {
            indexIds.add( indexProxy.getDescriptor().getId() );
        }
        return indexIds;
    }

    public ResourceIterator<File> snapshotIndexFiles() throws IOException
    {
        Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        for ( IndexProxy indexProxy : indexMapRef.getAllIndexProxies() )
        {
            snapshots.add( indexProxy.snapshotFiles() );
        }
        return Iterators.concatResourceIterators( snapshots.iterator() );
    }

    private IndexPopulationJob newIndexPopulationJob( EntityType type, boolean verifyBeforeFlipping )
    {
        MultipleIndexPopulator multiPopulator = multiPopulatorFactory.create( storeView, internalLogProvider, type, schemaState );
        return new IndexPopulationJob( multiPopulator, monitor, verifyBeforeFlipping );
    }

    private void startIndexPopulation( IndexPopulationJob job )
    {
        populationJobController.startIndexPopulation( job );
    }

    private String indexStateInfo( String tag, InternalIndexState state, StoreIndexDescriptor descriptor )
    {
        return format( "IndexingService.%s: index %d on %s is %s", tag, descriptor.getId(),
                descriptor.schema().userDescription( tokenNameLookup ), state.name() );
    }

    private void logIndexStateSummary( String method, Map<InternalIndexState,List<IndexLogRecord>> indexStates )
    {
        if ( indexStates.isEmpty() )
        {
            return;
        }
        int mostPopularStateCount = Integer.MIN_VALUE;
        InternalIndexState mostPopularState = null;
        for ( Map.Entry<InternalIndexState,List<IndexLogRecord>> indexStateEntry : indexStates.entrySet() )
        {
            if ( indexStateEntry.getValue().size() > mostPopularStateCount )
            {
                mostPopularState = indexStateEntry.getKey();
                mostPopularStateCount = indexStateEntry.getValue().size();
            }
        }
        indexStates.remove( mostPopularState );
        for ( Map.Entry<InternalIndexState,List<IndexLogRecord>> indexStateEntry : indexStates.entrySet() )
        {
            InternalIndexState state = indexStateEntry.getKey();
            List<IndexLogRecord> logRecords = indexStateEntry.getValue();
            for ( IndexLogRecord logRecord : logRecords )
            {
                internalLog.info( indexStateInfo( method, state, logRecord.getDescriptor() ) );
            }
        }
        internalLog.info( format( "IndexingService.%s: indexes not specifically mentioned above are %s", method, mostPopularState ) );
    }

    private void logIndexProviderSummary( Map<IndexProviderDescriptor,List<IndexLogRecord>> indexProviders )
    {
        Set<String> deprecatedIndexProviders = Arrays.stream( GraphDatabaseSettings.SchemaIndex.values() )
                .filter( GraphDatabaseSettings.SchemaIndex::deprecated )
                .map( GraphDatabaseSettings.SchemaIndex::providerName )
                .collect( Collectors.toSet() );
        StringJoiner joiner = new StringJoiner( ", ", "Deprecated index providers in use: ",
                ". Use procedure 'db.indexes()' to see what indexes use which index provider." );
        MutableBoolean anyDeprecated = new MutableBoolean();
        indexProviders.forEach( ( indexProviderDescriptor, indexLogRecords ) ->
        {
            if ( deprecatedIndexProviders.contains( indexProviderDescriptor.name() ) )
            {
                anyDeprecated.setTrue();
                int numberOfIndexes = indexLogRecords.size();
                joiner.add( indexProviderDescriptor.name() + " (" + numberOfIndexes + (numberOfIndexes == 1 ? " index" : " indexes") + ")" );
            }
        } );
        if ( anyDeprecated.getValue() )
        {
            userLog.info( joiner.toString() );
        }
    }

    private final class IndexPopulationStarter implements Function<IndexMap,IndexMap>
    {
        private final boolean verifyBeforeFlipping;
        private final StoreIndexDescriptor[] descriptors;
        private IndexPopulationJob nodePopulationJob;
        private IndexPopulationJob relationshipPopulationJob;

        IndexPopulationStarter( boolean verifyBeforeFlipping, StoreIndexDescriptor[] descriptors )
        {
            this.verifyBeforeFlipping = verifyBeforeFlipping;
            this.descriptors = descriptors;
        }

        @Override
        public IndexMap apply( IndexMap indexMap )
        {
            for ( StoreIndexDescriptor descriptor : descriptors )
            {
                if ( state == State.NOT_STARTED )
                {
                    // In case of recovery remove any previously recorded INDEX DROP for this particular index rule id,
                    // in some scenario where rule ids may be reused.
                    indexesToDropAfterCompletedRecovery.remove( descriptor.getId() );
                }
                IndexProxy index = indexMap.getIndexProxy( descriptor.getId() );
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
                    indexMap.putIndexProxy( index );
                    continue;
                }
                boolean flipToTentative = descriptor.canSupportUniqueConstraint();
                if ( state == State.RUNNING )
                {
                    if ( descriptor.schema().entityType() == EntityType.NODE )
                    {
                        nodePopulationJob = nodePopulationJob == null ? newIndexPopulationJob( EntityType.NODE, verifyBeforeFlipping ) : nodePopulationJob;
                        index = indexProxyCreator.createPopulatingIndexProxy( descriptor, flipToTentative, monitor,
                                nodePopulationJob );
                        index.start();
                    }
                    else
                    {
                        relationshipPopulationJob = relationshipPopulationJob == null ? newIndexPopulationJob( EntityType.RELATIONSHIP, verifyBeforeFlipping )
                                                                                      : relationshipPopulationJob;
                        index = indexProxyCreator.createPopulatingIndexProxy( descriptor, flipToTentative, monitor,
                                relationshipPopulationJob );
                        index.start();
                    }
                }
                else
                {
                    index = indexProxyCreator.createRecoveringIndexProxy( descriptor );
                }

                indexMap.putIndexProxy( index );
            }
            return indexMap;
        }

        void startPopulation()
        {
            if ( nodePopulationJob != null )
            {
                startIndexPopulation( nodePopulationJob );
            }
            if ( relationshipPopulationJob != null )
            {
                startIndexPopulation( relationshipPopulationJob );
            }
        }
    }

    private static final class IndexLogRecord
    {
        private final StoreIndexDescriptor descriptor;

        IndexLogRecord( StoreIndexDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        public long getIndexId()
        {
            return descriptor.getId();
        }

        public StoreIndexDescriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
