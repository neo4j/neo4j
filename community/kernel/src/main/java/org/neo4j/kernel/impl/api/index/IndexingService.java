/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * Manages neo4j indexes. Each index has an {@link IndexDescriptor}, which it uses to filter
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
public class IndexingService extends LifecycleAdapter implements IndexUpdateListener, IndexingProvidersService
{
    private static final String INDEX_SERVICE_INDEX_CLOSING_TAG = "indexServiceIndexClosing";
    private final IndexSamplingController samplingController;
    private final IndexProxyCreator indexProxyCreator;
    private final IndexStoreView storeView;
    private final IndexProviderMap providerMap;
    private final IndexMapReference indexMapRef;
    private final Iterable<IndexDescriptor> indexDescriptors;
    private final Log internalLog;
    private final Log userLog;
    private final IndexStatisticsStore indexStatisticsStore;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final String databaseName;
    private final boolean readOnly;
    private final Config config;
    private final TokenNameLookup tokenNameLookup;
    private final JobScheduler jobScheduler;
    private final LogProvider internalLogProvider;
    private final Monitor monitor;
    private final SchemaState schemaState;
    private final IndexPopulationJobController populationJobController;
    private static final String INIT_TAG = "Initialize IndexingService";

    enum State
    {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    public interface Monitor
    {
        void initialState( IndexDescriptor descriptor, InternalIndexState state );

        void populationCompleteOn( IndexDescriptor descriptor );

        void indexPopulationScanStarting();

        void indexPopulationScanComplete();

        void awaitingPopulationOfRecoveredIndex( IndexDescriptor descriptor );

        void indexSamplingTriggered( IndexSamplingMode mode );

        void populationCancelled();

        void populationJobCompleted( long peakDirectMemoryUsage );
    }

    public static class MonitorAdapter implements Monitor
    {
        @Override
        public void initialState( IndexDescriptor descriptor, InternalIndexState state )
        {   // Do nothing
        }

        @Override
        public void populationCompleteOn( IndexDescriptor descriptor )
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
        public void awaitingPopulationOfRecoveredIndex( IndexDescriptor descriptor )
        {   // Do nothing
        }

        @Override
        public void indexSamplingTriggered( IndexSamplingMode mode )
        {   // Do nothing
        }

        @Override
        public void populationCancelled()
        {   // Do nothing
        }

        @Override
        public void populationJobCompleted( long peakDirectMemoryUsage )
        {   // Do nothing
        }
    }

    public static final Monitor NO_MONITOR = new MonitorAdapter();

    private volatile State state = State.NOT_STARTED;

    IndexingService( IndexProxyCreator indexProxyCreator,
            IndexProviderMap providerMap,
            IndexMapReference indexMapRef,
            IndexStoreView storeView,
            Iterable<IndexDescriptor> indexDescriptors,
            IndexSamplingController samplingController,
            TokenNameLookup tokenNameLookup,
            JobScheduler scheduler,
            SchemaState schemaState,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            Monitor monitor,
            IndexStatisticsStore indexStatisticsStore,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker,
            String databaseName,
            boolean readOnly,
            Config config )
    {
        this.indexProxyCreator = indexProxyCreator;
        this.providerMap = providerMap;
        this.indexMapRef = indexMapRef;
        this.storeView = storeView;
        this.indexDescriptors = indexDescriptors;
        this.samplingController = samplingController;
        this.tokenNameLookup = tokenNameLookup;
        this.jobScheduler = scheduler;
        this.schemaState = schemaState;
        this.internalLogProvider = internalLogProvider;
        this.monitor = monitor;
        this.populationJobController = new IndexPopulationJobController( scheduler );
        this.internalLog = internalLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.indexStatisticsStore = indexStatisticsStore;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.databaseName = databaseName;
        this.readOnly = readOnly;
        this.config = config;
    }

    /**
     * Called while the database starts up, before recovery.
     */
    @Override
    public void init() throws IOException
    {
        validateDefaultProviderExisting();

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INIT_TAG ) )
        {
            indexMapRef.modify( indexMap ->
            {
                Map<InternalIndexState,List<IndexLogRecord>> indexStates = new EnumMap<>( InternalIndexState.class );
                for ( IndexDescriptor descriptor : indexDescriptors )
                {
                    IndexProxy indexProxy;

                    IndexProviderDescriptor providerDescriptor = descriptor.getIndexProvider();
                    IndexProvider provider = providerMap.lookup( providerDescriptor );
                    InternalIndexState initialState = provider.getInitialState( descriptor, cursorTracer );

                    indexStates.computeIfAbsent( initialState, internalIndexState -> new ArrayList<>() ).add( new IndexLogRecord( descriptor ) );

                    internalLog.debug( indexStateInfo( "init", initialState, descriptor ) );
                    switch ( initialState )
                    {
                    case ONLINE:
                        monitor.initialState( descriptor, ONLINE );
                        indexProxy = indexProxyCreator.createOnlineIndexProxy( descriptor );
                        break;
                    case POPULATING:
                        // The database was shut down during population, or a crash has occurred, or some other sad thing.
                        monitor.initialState( descriptor, POPULATING );
                        indexProxy = indexProxyCreator.createRecoveringIndexProxy( descriptor );
                        break;
                    case FAILED:
                        monitor.initialState( descriptor, FAILED );
                        IndexPopulationFailure failure = failure( provider.getPopulationFailure( descriptor, cursorTracer ) );
                        indexProxy = indexProxyCreator.createFailedIndexProxy( descriptor, failure );
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

        indexStatisticsStore.init();
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
    public void start() throws Exception
    {
        state = State.STARTING;

        // Recovery will not do refresh (update read views) while applying recovered transactions and instead
        // do it at one point after recovery... i.e. here
        indexMapRef.indexMapSnapshot().forEachIndexProxy( indexProxyOperation( "refresh", IndexProxy::refresh ) );

        final MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors = new LongObjectHashMap<>();
        indexMapRef.modify( indexMap ->
        {
            Map<InternalIndexState, List<IndexLogRecord>> indexStates = new EnumMap<>( InternalIndexState.class );
            Map<IndexProviderDescriptor,List<IndexLogRecord>> indexProviders = new HashMap<>();

            // Find all indexes that are not already online, do not require rebuilding, and create them
            indexMap.forEachIndexProxy( ( indexId, proxy ) ->
            {
                InternalIndexState state = proxy.getState();
                IndexDescriptor descriptor = proxy.getDescriptor();
                IndexProviderDescriptor providerDescriptor = descriptor.getIndexProvider();
                IndexLogRecord indexLogRecord = new IndexLogRecord( descriptor );
                indexStates.computeIfAbsent( state, internalIndexState -> new ArrayList<>() )
                        .add( indexLogRecord );
                indexProviders.computeIfAbsent( providerDescriptor, indexProviderDescriptor -> new ArrayList<>() )
                        .add( indexLogRecord );
                internalLog.debug( indexStateInfo( "start", state, descriptor ) );
                switch ( state )
                {
                case ONLINE:
                case FAILED:
                    proxy.start();
                    break;
                case POPULATING:
                    // Remember for rebuilding right below in this method
                    rebuildingDescriptors.put( indexId, descriptor );
                    break;
                default:
                    throw new IllegalStateException( "Unknown state: " + state );
                }
            } );
            logIndexStateSummary( "start", indexStates );
            logIndexProviderSummary( indexProviders );

            dontRebuildIndexesInReadOnlyMode( rebuildingDescriptors );
            // Drop placeholder proxies for indexes that need to be rebuilt
            dropRecoveringIndexes( indexMap, rebuildingDescriptors.keySet() );
            // Rebuild indexes by recreating and repopulating them
            populateIndexesOfAllTypes( rebuildingDescriptors, indexMap );

            return indexMap;
        } );

        indexStatisticsStore.start();
        samplingController.recoverIndexSamples();
        samplingController.start();

        // So at this point we've started population of indexes that needs to be rebuilt in the background.
        // Indexes backing uniqueness constraints are normally built within the transaction creating the constraint
        // and so we shouldn't leave such indexes in a populating state after recovery.
        // This is why we now go and wait for those indexes to be fully populated.
        rebuildingDescriptors.forEachKeyValue( ( indexId, index ) ->
                {
                    if ( !index.isUnique() )
                    {
                        // It's not a uniqueness constraint, so don't wait for it to be rebuilt
                        return;
                    }

                    IndexProxy proxy;
                    try
                    {
                        proxy = getIndexProxy( index );
                    }
                    catch ( IndexNotFoundKernelException e )
                    {
                        throw new IllegalStateException( "What? This index was seen during recovery just now, why isn't it available now?", e );
                    }

                    if ( proxy.getDescriptor().getOwningConstraintId().isEmpty() )
                    {
                        // Even though this is an index backing a uniqueness constraint, the uniqueness constraint wasn't created
                        // so there's no gain in waiting for this index.
                        return;
                    }

                    monitor.awaitingPopulationOfRecoveredIndex( index );
                    awaitOnlineAfterRecovery( proxy );
                } );

        state = State.RUNNING;
    }

    private void dontRebuildIndexesInReadOnlyMode( MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors )
    {
        if ( readOnly && rebuildingDescriptors.notEmpty() )
        {
            String indexString = rebuildingDescriptors.values().stream()
                    .map( String::valueOf )
                    .collect( Collectors.joining( ", ", "{", "}" ) );
            throw new IllegalStateException(
                    "Some indexes need to be rebuilt. This is not allowed in read only mode. Please start db in writable mode to rebuild indexes. " +
                            "Indexes needing rebuild: " + indexString );
        }
    }

    private void populateIndexesOfAllTypes( MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors, IndexMap indexMap )
    {
        Map<EntityType,MutableLongObjectMap<IndexDescriptor>> rebuildingDescriptorsByType = new EnumMap<>( EntityType.class );
        for ( IndexDescriptor descriptor : rebuildingDescriptors )
        {
            rebuildingDescriptorsByType.computeIfAbsent( descriptor.schema().entityType(), type -> new LongObjectHashMap<>() )
                    .put( descriptor.getId(), descriptor );
        }

        for ( Map.Entry<EntityType,MutableLongObjectMap<IndexDescriptor>> descriptorToPopulate : rebuildingDescriptorsByType.entrySet() )
        {
            IndexPopulationJob populationJob = newIndexPopulationJob( descriptorToPopulate.getKey(), false, SYSTEM );
            populate( descriptorToPopulate.getValue(), indexMap, populationJob );
        }
    }

    private void populate( MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors, IndexMap indexMap, IndexPopulationJob populationJob )
    {
        rebuildingDescriptors.forEachKeyValue( ( indexId, descriptor ) ->
        {
            boolean flipToTentative = false; // Never pass through a tentative online state during recovery.
            IndexProxy proxy = indexProxyCreator.createPopulatingIndexProxy( descriptor, flipToTentative, monitor, populationJob );
            proxy.start();
            indexMap.putIndexProxy( proxy );
        } );
        startIndexPopulation( populationJob );
    }

    /**
     * Polls the {@link IndexProxy#getState() state of the index} and waits for it to be either {@link InternalIndexState#ONLINE},
     * in which case the wait is over, or {@link InternalIndexState#FAILED}, in which an exception is logged.
     *
     * This method is only called during startup, and might be called as part of recovery. If we threw an exception here, it could
     * render the database unrecoverable. That's why we only log a message about failed indexes.
     */
    private void awaitOnlineAfterRecovery( IndexProxy proxy )
    {
        while ( true )
        {
            switch ( proxy.getState() )
            {
            case ONLINE:
                return;
            case FAILED:
                String message =
                        String.format( "Index %s entered %s state while recovery waited for it to be fully populated.", proxy.getDescriptor(), FAILED );
                IndexPopulationFailure populationFailure = proxy.getPopulationFailure();
                String causeOfFailure = populationFailure.asString();
                // Log as INFO because at this point we don't know if the constraint index was ever bound to a constraint or not.
                // If it was really bound to a constraint, then we actually ought to log as WARN or ERROR, I suppose.
                // But by far the most likely scenario is that the constraint itself was never created.
                internalLog.info( IndexPopulationFailure.appendCauseOfFailure( message, causeOfFailure ) );
                return;
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

    // while indexes will be closed on shutdown we need to stop ongoing jobs before we will start shutdown to prevent
    // races between checkpoint flush and index jobs
    @Override
    public void stop() throws Exception
    {
        samplingController.stop();
        populationJobController.stop();
        indexStatisticsStore.stop();
    }

    // We need to stop indexing service on shutdown since we can have transactions that are ongoing/finishing
    // after we start stopping components and those transactions should be able to finish successfully
    @Override
    public void shutdown() throws IOException
    {
        state = State.STOPPED;
        closeAllIndexes();
        indexStatisticsStore.shutdown();
    }

    @Override
    public void validateBeforeCommit( IndexDescriptor index, Value[] tuple, long entityId )
    {
        indexMapRef.validateBeforeCommit( index, tuple, entityId );
    }

    @Override
    public void validateIndexPrototype( IndexPrototype prototype )
    {
        IndexProvider provider = providerMap.lookup( prototype.getIndexProvider() );
        provider.validatePrototype( prototype );
    }

    @Override
    public IndexProviderDescriptor getDefaultProvider()
    {
        return providerMap.getDefaultProvider().getProviderDescriptor();
    }

    @Override
    public IndexProviderDescriptor getFulltextProvider()
    {
        return providerMap.getFulltextProvider().getProviderDescriptor();
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        return providerMap.completeConfiguration( index );
    }

    @Override
    public IndexProviderDescriptor indexProviderByName( String providerName )
    {
        return providerMap.lookup( providerName ).getProviderDescriptor();
    }

    /**
     * Applies the given updates, which may contain updates for one or more indexes.
     *
     * @param updates {@link IndexEntryUpdate updates} to apply.
     * @throws UncheckedIOException potentially thrown from index updating.
     * @throws KernelException potentially thrown from index updating.
     */
    @Override
    public void applyUpdates( Iterable<IndexEntryUpdate<IndexDescriptor>> updates, PageCursorTracer cursorTracer ) throws KernelException
    {
        if ( state == State.NOT_STARTED )
        {
            // We're in recovery, which means we'll be telling indexes to apply with additional care for making
            // idempotent changes.
            apply( updates, IndexUpdateMode.RECOVERY, cursorTracer );
        }
        else if ( state == State.RUNNING || state == State.STARTING )
        {
            apply( updates, IndexUpdateMode.ONLINE, cursorTracer );
        }
        else
        {
            throw new IllegalStateException(
                    "Can't apply index updates " + asList( updates ) + " while indexing service is " + state );
        }
    }

    private void apply( Iterable<IndexEntryUpdate<IndexDescriptor>> updates, IndexUpdateMode updateMode, PageCursorTracer cursorTracer ) throws KernelException
    {
        try ( IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( updateMode ) )
        {
            for ( IndexEntryUpdate<IndexDescriptor> indexUpdate : updates )
            {
                processUpdate( updaterMap, indexUpdate, cursorTracer );
            }
        }
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
     *
     * @param subject subject that triggered the index creation.
     * This is used for monitoring purposes, so work related to index creation and population can be linked to its originator.
     */
    @Override
    public void createIndexes( Subject subject, IndexDescriptor... rules )
    {
        createIndexes( false, subject, rules );
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
    public void createIndexes( boolean verifyBeforeFlipping, Subject subject, IndexDescriptor... rules )
    {
        IndexPopulationStarter populationStarter = new IndexPopulationStarter( verifyBeforeFlipping, subject, rules );
        indexMapRef.modify( populationStarter );
        populationStarter.startPopulation();
    }

    private void processUpdate( IndexUpdaterMap updaterMap, IndexEntryUpdate<IndexDescriptor> indexUpdate,
            PageCursorTracer cursorTracer ) throws IndexEntryConflictException
    {
        IndexUpdater updater = updaterMap.getUpdater( indexUpdate.indexKey(), cursorTracer );
        if ( updater != null )
        {
            updater.process( indexUpdate );
        }
    }

    @Override
    public void dropIndex( IndexDescriptor rule )
    {
        Preconditions.checkState( state == State.RUNNING || state == State.NOT_STARTED, "Dropping index in unexpected state %s", state.name() );
        indexMapRef.modify( indexMap ->
        {
            long indexId = rule.getId();
            IndexProxy index = indexMap.removeIndexProxy( indexId );

            if ( state == State.RUNNING )
            {
                Preconditions.checkState( index != null, "Index %s doesn't exists", rule );
                index.drop();
            }
            else if ( index != null )
            {
                try
                {
                    index.drop();
                }
                catch ( Exception e )
                {
                    // This is OK to get during recovery because the underlying index can be in any unknown state
                    // while we're recovering. Let's just move on to closing it instead.
                    try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_SERVICE_INDEX_CLOSING_TAG ) )
                    {
                        index.close( cursorTracer );
                    }
                    catch ( IOException closeException )
                    {
                        // This is OK for the same reason as above
                    }
                }
            }
            return indexMap;
        } );
    }

    public void triggerIndexSampling( IndexSamplingMode mode )
    {
        internalLog.info( "Manual trigger for sampling all indexes [" + mode + "]" );
        monitor.indexSamplingTriggered( mode );
        samplingController.sampleIndexes( mode );
    }

    public void triggerIndexSampling( IndexDescriptor index, IndexSamplingMode mode )
    {
        String description = index.userDescription( tokenNameLookup );
        internalLog.info( "Manual trigger for sampling index " + description + " [" + mode + "]" );
        samplingController.sampleIndex( index.getId(), mode );
    }

    private static void dropRecoveringIndexes( IndexMap indexMap, LongIterable indexesToRebuild )
    {
        indexesToRebuild.forEach( idx ->
        {
            IndexProxy indexProxy = indexMap.removeIndexProxy( idx );
            assert indexProxy != null;
            indexProxy.drop();
        } );
    }

    @Override
    public void activateIndex( IndexDescriptor descriptor ) throws
            IndexNotFoundKernelException, IndexActivationFailedKernelException, IndexPopulationFailedKernelException
    {
        try
        {
            if ( state == State.RUNNING ) // don't do this during recovery.
            {
                IndexProxy index = getIndexProxy( descriptor );
                index.awaitStoreScanCompleted( 0, TimeUnit.MILLISECONDS );
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

    public IndexProxy getIndexProxy( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexProxy( index.getId() );
    }

    @Deprecated
    public IndexProxy getIndexProxy( long indexId ) throws IndexNotFoundKernelException
    {
        return indexMapRef.getIndexProxy( indexId );
    }

    @Override
    public void validateIndex( long indexId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException,
            UniquePropertyValueValidationException
    {
        indexMapRef.getIndexProxy( indexId ).validate();
    }

    public void forceAll( IOLimiter limiter, PageCursorTracer cursorTracer ) throws IOException
    {
        indexStatisticsStore.checkpoint( limiter, cursorTracer );
        indexMapRef.indexMapSnapshot().forEachIndexProxy( indexProxyOperation( "force", proxy -> proxy.force( limiter, cursorTracer ) ) );
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
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_SERVICE_INDEX_CLOSING_TAG ) )
        {
            indexMapRef.modify( indexMap ->
            {
                Iterable<IndexProxy> indexesToStop = indexMap.getAllIndexProxies();
                for ( IndexProxy index : indexesToStop )
                {
                    try
                    {
                        index.close( cursorTracer );
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

    public ResourceIterator<Path> snapshotIndexFiles() throws IOException
    {
        Collection<ResourceIterator<Path>> snapshots = new ArrayList<>();
        snapshots.add( asResourceIterator( iterator( indexStatisticsStore.storeFile() ) ) );
        for ( IndexProxy indexProxy : indexMapRef.getAllIndexProxies() )
        {
            snapshots.add( indexProxy.snapshotFiles() );
        }
        return Iterators.concatResourceIterators( snapshots.iterator() );
    }

    private IndexPopulationJob newIndexPopulationJob( EntityType type, boolean verifyBeforeFlipping, Subject subject )
    {
        MultipleIndexPopulator multiPopulator = new MultipleIndexPopulator( storeView, internalLogProvider, type, schemaState, indexStatisticsStore,
                jobScheduler, tokenNameLookup, pageCacheTracer, memoryTracker, databaseName, subject, config );
        return new IndexPopulationJob( multiPopulator, monitor, verifyBeforeFlipping, pageCacheTracer, memoryTracker, databaseName, subject, NODE, config );
    }

    private void startIndexPopulation( IndexPopulationJob job )
    {
        if ( storeView.isEmpty() )
        {
            // Creating indexes and constraints on an empty database, before ingesting data doesn't need to do unnecessary scheduling juggling,
            // instead just run it on the caller thread.
            job.run();
        }
        else
        {
            populationJobController.startIndexPopulation( job );
        }
    }

    private String indexStateInfo( String tag, InternalIndexState state, IndexDescriptor descriptor )
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

    private final class IndexPopulationStarter implements UnaryOperator<IndexMap>
    {
        private final boolean verifyBeforeFlipping;
        private final Subject subject;
        private final IndexDescriptor[] descriptors;
        private IndexPopulationJob nodePopulationJob;
        private IndexPopulationJob relationshipPopulationJob;

        IndexPopulationStarter( boolean verifyBeforeFlipping, Subject subject, IndexDescriptor[] descriptors )
        {
            this.verifyBeforeFlipping = verifyBeforeFlipping;
            this.subject = subject;
            this.descriptors = descriptors;
        }

        @Override
        public IndexMap apply( IndexMap indexMap )
        {
            for ( IndexDescriptor descriptor : descriptors )
            {
                IndexProxy index = indexMap.getIndexProxy( descriptor );
                if ( index != null && state == State.NOT_STARTED )
                {
                    // This index already has a proxy. No need to build another.
                    continue;
                }
                boolean flipToTentative = descriptor.isUnique();
                if ( state == State.RUNNING )
                {
                    if ( descriptor.schema().entityType() == NODE )
                    {
                        nodePopulationJob =
                                nodePopulationJob == null ? newIndexPopulationJob( NODE, verifyBeforeFlipping, subject ) : nodePopulationJob;
                        index = indexProxyCreator.createPopulatingIndexProxy( descriptor, flipToTentative, monitor,
                                nodePopulationJob );
                        index.start();
                    }
                    else
                    {
                        relationshipPopulationJob = relationshipPopulationJob == null ? newIndexPopulationJob( RELATIONSHIP, verifyBeforeFlipping, subject )
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
        private final IndexDescriptor descriptor;

        IndexLogRecord( IndexDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        public long getIndexId()
        {
            return descriptor.getId();
        }

        public IndexDescriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
