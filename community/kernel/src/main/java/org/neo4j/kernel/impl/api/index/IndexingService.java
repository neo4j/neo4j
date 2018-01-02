/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.function.BiConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingControllerFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.concatResourceIterators;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.kernel.api.index.InternalIndexState.FAILED;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

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
public class IndexingService extends LifecycleAdapter implements PrimitiveLongVisitor<RuntimeException>
{
    private final IndexSamplingController samplingController;
    private final IndexProxySetup proxySetup;
    private final IndexStoreView storeView;
    private final SchemaIndexProviderMap providerMap;
    private final IndexMapReference indexMapRef;
    private final Iterable<IndexRule> indexRules;
    private final Log log;
    private final TokenNameLookup tokenNameLookup;
    private final Monitor monitor;
    private final PrimitiveLongSet recoveredNodeIds = Primitive.longSet( 20 );

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

    public static abstract class MonitorAdapter implements Monitor
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

    public static final Monitor NO_MONITOR = new MonitorAdapter()
    {
    };

    private volatile State state = State.NOT_STARTED;

    // use IndexService.create do not instantiate manually
    protected IndexingService( IndexProxySetup proxySetup,
                               SchemaIndexProviderMap providerMap,
                               IndexMapReference indexMapRef,
                               IndexStoreView storeView,
                               Iterable<IndexRule> indexRules,
                               IndexSamplingController samplingController,
                               TokenNameLookup tokenNameLookup,
                               LogProvider logProvider,
                               Monitor monitor )
    {
        this.proxySetup = proxySetup;
        this.providerMap = providerMap;
        this.indexMapRef = indexMapRef;
        this.storeView = storeView;
        this.indexRules = indexRules;
        this.samplingController = samplingController;
        this.tokenNameLookup = tokenNameLookup;
        this.monitor = monitor;
        this.log = logProvider.getLog( getClass() );
    }

    public static IndexingService create( IndexSamplingConfig samplingConfig,
                                          JobScheduler scheduler,
                                          SchemaIndexProviderMap providerMap,
                                          IndexStoreView storeView,
                                          TokenNameLookup tokenNameLookup,
                                          UpdateableSchemaState updateableSchemaState,
                                          Iterable<IndexRule> indexRules,
                                          LogProvider logProvider, Monitor monitor )
    {
        if ( providerMap == null || providerMap.getDefaultProvider() == null )
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without an index provider, " +
                    "please make sure that a valid provider (subclass of " + SchemaIndexProvider.class.getName() +
                    ") is on your classpath." );
        }

        IndexMapReference indexMapRef = new IndexMapReference();
        IndexSamplingControllerFactory factory =
                new IndexSamplingControllerFactory( samplingConfig, storeView, scheduler, tokenNameLookup, logProvider );
        IndexSamplingController indexSamplingController = factory.create( indexMapRef );
        IndexProxySetup proxySetup = new IndexProxySetup(
                samplingConfig, storeView, providerMap, updateableSchemaState, tokenNameLookup, scheduler, logProvider
        );

        return new IndexingService( proxySetup, providerMap, indexMapRef, storeView, indexRules,
                indexSamplingController, tokenNameLookup, logProvider, monitor );
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
            log.info( proxySetup.indexStateInfo( "init", indexId, initialState, descriptor ) );
            boolean constraint = indexRule.isConstraintIndex();

            switch ( initialState )
            {
                case ONLINE:
                    indexProxy =
                        proxySetup.createOnlineIndexProxy( indexId, descriptor, providerDescriptor, constraint );
                    break;
                case POPULATING:
                    // The database was shut down during population, or a crash has occurred, or some other sad thing.
                    if ( constraint && indexRule.getOwningConstraint() == null )
                    {
                        // don't bother rebuilding if we are going to throw the index away anyhow
                        indexProxy = proxySetup.createFailedIndexProxy( indexId, descriptor, providerDescriptor, constraint,
                                failure( "Constraint for index was not committed." ) );
                    }
                    else
                    {
                        indexProxy = proxySetup.createRecoveringIndexProxy( descriptor, providerDescriptor, constraint );
                    }
                    break;
                case FAILED:
                    IndexPopulationFailure failure = failure( provider.getPopulationFailure( indexId ) );
                    indexProxy = proxySetup.createFailedIndexProxy( indexId, descriptor, providerDescriptor, constraint, failure );
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
        indexMap.foreachIndexProxy( new BiConsumer<Long, IndexProxy>()
        {
            @Override
            public void accept( Long indexId, IndexProxy proxy )
            {
                InternalIndexState state = proxy.getState();
                IndexDescriptor descriptor = proxy.getDescriptor();
                log.info( proxySetup.indexStateInfo( "start", indexId, state, descriptor ) );
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
                }
            }
        } );

        // Drop placeholder proxies for indexes that need to be rebuilt
        dropRecoveringIndexes( indexMap, rebuildingDescriptors.keySet() );

        // Rebuild indexes by recreating and repopulating them
        for ( Map.Entry<Long,RebuildingIndexDescriptor> entry : rebuildingDescriptors.entrySet() )
        {
            long indexId = entry.getKey();
            RebuildingIndexDescriptor descriptor = entry.getValue();

            IndexProxy proxy = proxySetup.createPopulatingIndexProxy(
                    indexId,
                    descriptor.getIndexDescriptor(),
                    descriptor.getProviderDescriptor(),
                    descriptor.getConfiguration(),
                    false, // never pass through a tentative online state during recovery
                    monitor );
            proxy.start();
            indexMap.putIndexProxy( indexId, proxy );
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
                proxy = getIndexProxy( entry.getKey().longValue() );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new ThisShouldNotHappenError( "Mattias",
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
            case ONLINE: return;
            case FAILED: throw new IllegalStateException( "Index entered " + FAILED +
                    " state while recovery waited for it to be fully populated" );
            case POPULATING:
                // Sleep a short while and look at state again the next loop iteration
                Thread.sleep( 10 );
                break;
            default: throw new IllegalStateException( proxy.getState().name() );
            }
        }
    }

    @Override
    public void stop()
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

    /*
     * Creates an index.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     */
    public void createIndex( IndexRule rule )
    {
        IndexMap indexMap = indexMapRef.indexMapSnapshot();

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
            return;
        }
        final IndexDescriptor descriptor = new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
        SchemaIndexProvider.Descriptor providerDescriptor = rule.getProviderDescriptor();
        boolean constraint = rule.isConstraintIndex();
        if ( state == State.RUNNING )
        {
            try
            {
                index = proxySetup.createPopulatingIndexProxy(
                        ruleId, descriptor, providerDescriptor, new IndexConfiguration( constraint ), constraint, monitor );
                index.start();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            index = proxySetup.createRecoveringIndexProxy( descriptor, providerDescriptor, constraint );
        }

        indexMap.putIndexProxy( rule.getId(), index );
        indexMapRef.setIndexMap( indexMap );
    }

    @Override
    public boolean visited( long recoveredNodeId ) throws RuntimeException
    {
        if ( state != State.NOT_STARTED )
        {
            throw new IllegalStateException(
                    "Can't queue recovered node ids " + recoveredNodeId + " while indexing service is " + state );
        }

        recoveredNodeIds.add( recoveredNodeId );
        return false;
    }

    public ValidatedIndexUpdates validate( Iterable<NodePropertyUpdate> updates, IndexUpdateMode updateMode )
    {
        if ( state != State.STARTING && state != State.RUNNING )
        {
            throw new IllegalStateException(
                    "Can't validate index updates " + toList( updates ) + " while indexing service is " + state );
        }

        IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( updateMode );

        boolean updaterMapShouldBeClosed = true;
        try
        {
            Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex =
                    groupUpdatesByIndexDescriptor( updates, updaterMap );

            if ( updatesByIndex.isEmpty() )
            {
                return ValidatedIndexUpdates.NONE;
            }

            AggregatedReservation aggregatedReservation = new AggregatedReservation( updatesByIndex.size() );
            for ( Map.Entry<IndexDescriptor,List<NodePropertyUpdate>> entry : updatesByIndex.entrySet() )
            {
                validateAndRecordReservation( entry.getValue(), aggregatedReservation, updaterMap,
                        entry.getKey() );
            }

            ValidatedIndexUpdates validatedUpdates =
                    newValidatedIndexUpdates( updaterMap, updatesByIndex, aggregatedReservation );

            updaterMapShouldBeClosed = false;

            return validatedUpdates;
        }
        finally
        {
            if ( updaterMapShouldBeClosed )
            {
                updaterMap.close();
            }
        }
    }

    private void validateAndRecordReservation( List<NodePropertyUpdate> indexUpdates,
            AggregatedReservation aggregatedReservation, IndexUpdaterMap updaterMap, IndexDescriptor descriptor )
    {
        boolean exceptionThrown = false;
        try
        {
            IndexUpdater updater = updaterMap.getUpdater( descriptor );
            Reservation reservation = updater.validate( indexUpdates );
            aggregatedReservation.add( reservation );
        }
        catch ( IOException | IndexCapacityExceededException e )
        {
            exceptionThrown = true;
            String indexName = descriptor.userDescription( tokenNameLookup );
            throw new UnderlyingStorageException( "Validation of updates for index " + indexName + " failed", e );
        }
        catch ( Throwable t )
        {
            exceptionThrown = true;
            throw t;
        }
        finally
        {
            if ( exceptionThrown )
            {
                aggregatedReservation.release();
            }
        }
    }

    private static ValidatedIndexUpdates newValidatedIndexUpdates( final IndexUpdaterMap indexUpdaters,
            final Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex, final Reservation reservation )
    {
        return new ValidatedIndexUpdates()
        {
            @Override
            public void flush() throws IOException, IndexEntryConflictException, IndexCapacityExceededException
            {
                for ( Map.Entry<IndexDescriptor,List<NodePropertyUpdate>> entry : updatesByIndex.entrySet() )
                {
                    IndexDescriptor indexDescriptor = entry.getKey();
                    List<NodePropertyUpdate> updates = entry.getValue();

                    IndexUpdater updater = indexUpdaters.getUpdater( indexDescriptor );
                    for ( NodePropertyUpdate update : updates )
                    {
                        updater.process( update );
                    }
                }
            }

            @Override
            public void close()
            {
                try
                {
                    reservation.release();
                }
                finally
                {
                    indexUpdaters.close();
                }
            }

            @Override
            public boolean hasChanges()
            {
                return !updatesByIndex.isEmpty();
            }
        };
    }

    private void applyRecoveredUpdates() throws IOException
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Applying recovered updates: " + recoveredNodeIds );
        }
        monitor.applyingRecoveredData( recoveredNodeIds );
        if ( !recoveredNodeIds.isEmpty() )
        {
            try ( IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap( IndexUpdateMode.BATCHED ) )
            {
                for ( IndexUpdater updater : updaterMap )
                {
                    updater.remove( recoveredNodeIds );
                }
            }

            List<NodePropertyUpdate> recoveredUpdates = readRecoveredUpdatesFromStore();

            try ( ValidatedIndexUpdates validatedUpdates = validate( recoveredUpdates, IndexUpdateMode.BATCHED ) )
            {
                validatedUpdates.flush();
                monitor.appliedRecoveredData( recoveredUpdates );
            }
            catch ( IndexEntryConflictException | IndexCapacityExceededException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
        recoveredNodeIds.clear();
    }

    private List<NodePropertyUpdate> readRecoveredUpdatesFromStore()
    {
        final List<NodePropertyUpdate> recoveredUpdates = new ArrayList<>();

        recoveredNodeIds.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long nodeId )
            {
                Iterables.addAll( recoveredUpdates, storeView.nodeAsUpdates( nodeId ) );
                return false;
            }
        } );

        return recoveredUpdates;
    }

    private static Map<IndexDescriptor,List<NodePropertyUpdate>> groupUpdatesByIndexDescriptor(
            Iterable<NodePropertyUpdate> updates, IndexUpdaterMap updaterMap )
    {
        int numberOfIndexes = updaterMap.numberOfIndexes();
        Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex = new HashMap<>( numberOfIndexes, 1 );

        for ( NodePropertyUpdate update : updates )
        {
            int propertyKeyId = update.getPropertyKeyId();
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                for ( int len = update.getNumberOfLabelsAfter(), i = 0; i < len; i++ )
                {
                    IndexDescriptor descriptor = new IndexDescriptor( update.getLabelAfter( i ), propertyKeyId );
                    storeUpdateIfIndexExists( updaterMap, update, descriptor, updatesByIndex );
                }
                break;

            case REMOVED:
                for ( int len = update.getNumberOfLabelsBefore(), i = 0; i < len; i++ )
                {
                    IndexDescriptor descriptor = new IndexDescriptor( update.getLabelBefore( i ), propertyKeyId );
                    storeUpdateIfIndexExists( updaterMap, update, descriptor, updatesByIndex );
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
                        IndexDescriptor descriptor = new IndexDescriptor( labelAfter, propertyKeyId );
                        storeUpdateIfIndexExists( updaterMap, update, descriptor, updatesByIndex );
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
            }
        }

        return updatesByIndex;
    }

    private static void storeUpdateIfIndexExists( IndexUpdaterMap updaterMap, NodePropertyUpdate update,
            IndexDescriptor descriptor, Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex )
    {
        IndexUpdater updater = updaterMap.getUpdater( descriptor );
        if ( updater != null )
        {
            List<NodePropertyUpdate> indexUpdates = updatesByIndex.get( descriptor );
            if ( indexUpdates == null )
            {
                updatesByIndex.put( descriptor, indexUpdates = new ArrayList<>() );
            }
            indexUpdates.add( update );
        }
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

        return concatResourceIterators( snapshots.iterator() );
    }

    // This is currently exposed for use by tests only
    public IndexSamplingController samplingController()
    {
        return samplingController;
    }
}
