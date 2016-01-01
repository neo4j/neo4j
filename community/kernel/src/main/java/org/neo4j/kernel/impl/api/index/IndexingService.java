/**
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.BiConsumer;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.Iterables.concatResourceIterators;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * Manages the indexes that were introduced in 2.0. These indexes depend on the normal neo4j logical log for
 * transactionality. Each index has an {@link org.neo4j.kernel.impl.nioneo.store.IndexRule}, which it uses to filter
 * changes that come into the database. Changes that apply to the the rule are indexed. This way, "normal" changes to
 * the database can be replayed to perform recovery after a crash.
 * <p/>
 * <h3>Recovery procedure</h3>
 * <p/>
 * Each index has a state, as defined in {@link org.neo4j.kernel.api.index.InternalIndexState}, which is used during
 * recovery. If an index is anything but {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, it will simply be
 * destroyed and re-created.
 * <p/>
 * If, however, it is {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, the index provider is required to
 * also guarantee that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter
{
    private final IndexMapReference indexMapReference = new IndexMapReference();

    private final JobScheduler scheduler;
    private final SchemaIndexProviderMap providerMap;
    private final IndexStoreView storeView;
    private final TokenNameLookup tokenNameLookup;
    private final Logging logging;
    private final StringLogger logger;
    private final UpdateableSchemaState updateableSchemaState;
    private final Set<Long> recoveredNodeIds = new HashSet<>();
    private final Monitor monitor;

    enum State
    {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    public interface Monitor
    {
        void applyingRecoveredData( Collection<Long> nodeIds );

        void appliedRecoveredData( Iterable<NodePropertyUpdate> updates );

        void verifyDeferredConstraints();
    }

    public static abstract class MonitorAdapter implements Monitor
    {
        @Override
        public void appliedRecoveredData( Iterable<NodePropertyUpdate> updates )
        {   // Do nothing
        }

        @Override
        public void applyingRecoveredData( Collection<Long> nodeIds )
        {   // Do nothing
        }

        @Override
        public void verifyDeferredConstraints()
        {
            // Do nothing
        }
    }

    public static final Monitor NO_MONITOR = new MonitorAdapter()
    {
    };

    private volatile State state = State.NOT_STARTED;


    public IndexingService( JobScheduler scheduler,
                            SchemaIndexProviderMap providerMap,
                            IndexStoreView storeView,
                            TokenNameLookup tokenNameLookup,
                            UpdateableSchemaState updateableSchemaState,
                            Logging logging, Monitor monitor )
    {
        this.scheduler = scheduler;
        this.providerMap = providerMap;
        this.storeView = storeView;
        this.logging = logging;
        this.monitor = monitor;
        this.logger = logging.getMessagesLog( getClass() );
        this.updateableSchemaState = updateableSchemaState;
        this.tokenNameLookup = tokenNameLookup;

        if ( providerMap == null || providerMap.getDefaultProvider() == null )
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without an index provider, " +
                    "please make sure that a valid provider (subclass of " + SchemaIndexProvider.class.getName() +
                    ") is on your classpath." );
        }
    }

    /**
     * Called while the database starts up, before recovery.
     *
     * @param indexRules Known index rules before recovery.
     */
    public void initIndexes( Iterator<IndexRule> indexRules )
    {
        IndexMap indexMap = indexMapReference.getIndexMapCopy();

        for ( IndexRule indexRule : loop( indexRules ) )
        {
            IndexProxy indexProxy;

            long indexId = indexRule.getId();
            IndexDescriptor descriptor = createDescriptor( indexRule );
            SchemaIndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
            SchemaIndexProvider provider = providerMap.apply( providerDescriptor );
            InternalIndexState initialState = provider.getInitialState( indexId );

            logger.info( format( "IndexingService.initIndexes: index on %s is %s",
                    descriptor.userDescription( tokenNameLookup ), initialState ) );

            boolean constraint = indexRule.isConstraintIndex();

            switch ( initialState )
            {
                case ONLINE:
                    indexProxy =
                        createAndStartOnlineIndexProxy( indexId, descriptor, providerDescriptor, constraint );
                    break;
                case POPULATING:
                    // The database was shut down during population, or a crash has occurred, or some other sad thing.
                    indexProxy = createAndStartRecoveringIndexProxy( descriptor, providerDescriptor );
                    break;
                case FAILED:
                    IndexPopulationFailure failure = failure( provider.getPopulationFailure( indexId ) );
                    indexProxy =
                        createAndStartFailedIndexProxy( indexId, descriptor, providerDescriptor, constraint, failure );
                    break;
                default:
                    throw new IllegalArgumentException( "" + initialState );
            }
            indexMap.putIndexProxy( indexId, indexProxy );
        }

        indexMapReference.setIndexMap( indexMap );
    }

    // Recovery semantics: This is to be called after initIndexes, and after the database has run recovery.
    public void startIndexes() throws IOException
    {
        state = State.STARTING;

        applyRecoveredUpdates();
        IndexMap indexMap = indexMapReference.getIndexMapCopy();

        final Map<Long, Pair<IndexDescriptor, SchemaIndexProvider.Descriptor>> rebuildingDescriptors = new HashMap<>();

        // Find all indexes that are not already online, do not require rebuilding, and create them
        indexMap.foreachIndexProxy( new BiConsumer<Long, IndexProxy>()
        {
            @Override
            public void accept( Long indexId, IndexProxy indexProxy )
            {
                InternalIndexState state = indexProxy.getState();
                logger.info( String.format( "IndexingService.start: index on %s is %s",
                        indexProxy.getDescriptor().userDescription( tokenNameLookup ), state.name() ) );
                switch ( state )
                {
                    case ONLINE:
                        // Don't do anything, index is ok.
                        break;
                    case POPULATING:
                        // Remember for rebuilding
                        rebuildingDescriptors.put( indexId, getIndexProxyDescriptors( indexProxy ) );
                        break;
                    case FAILED:
                        // Don't do anything, the user needs to drop the index and re-create
                        break;
                }
            }
        } );

        // Drop placeholder proxies for indexes that need to be rebuilt
        dropRecoveringIndexes( indexMap, rebuildingDescriptors );

        // Rebuild indexes by recreating and repopulating them
        for ( Map.Entry<Long, Pair<IndexDescriptor, SchemaIndexProvider.Descriptor>> entry :
                rebuildingDescriptors.entrySet() )
        {
            long indexId = entry.getKey();
            Pair<IndexDescriptor, SchemaIndexProvider.Descriptor> descriptors = entry.getValue();
            IndexDescriptor indexDescriptor = descriptors.first();
            SchemaIndexProvider.Descriptor providerDescriptor = descriptors.other();

            /*
             * Passing in "false" for unique here may seem surprising, and.. well, yes, it is, I was surprised too.
             * However, it is actually perfectly safe, because whenever we have constraint indexes here, they will
             * be in a state where they didn't finish populating, and despite the fact that we re-create them here,
             * they will get dropped as soon as recovery is completed by the constraint system.
             */
            IndexProxy indexProxy =
                createAndStartPopulatingIndexProxy( indexId, indexDescriptor, providerDescriptor, false );
            indexMap.putIndexProxy( indexId, indexProxy );
        }

        indexMapReference.setIndexMap( indexMap );
        state = State.RUNNING;
    }

    @Override
    public void stop()
    {
        state = State.STOPPED;
        closeAllIndexes();
    }

    public IndexProxy getProxyForRule( long indexId ) throws IndexNotFoundKernelException
    {
        IndexProxy indexProxy = indexMapReference.getIndexProxy( indexId );
        if ( indexProxy == null )
        {
            throw new IndexNotFoundKernelException( "No index with id " + indexId + " exists." );
        }
        return indexProxy;
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
        IndexMap indexMap = indexMapReference.getIndexMapCopy();

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
            indexMapReference.setIndexMap( indexMap );
            return;
        }

        final IndexDescriptor descriptor = createDescriptor( rule );
        SchemaIndexProvider.Descriptor providerDescriptor = rule.getProviderDescriptor();
        boolean constraint = rule.isConstraintIndex();
        if ( state == State.RUNNING )
        {
            try
            {
                index = createAndStartPopulatingIndexProxy( ruleId, descriptor, providerDescriptor, constraint );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            index = createAndStartRecoveringIndexProxy( descriptor, providerDescriptor );
        }

        indexMap.putIndexProxy( rule.getId(), index );
        indexMapReference.setIndexMap( indexMap );
    }

    private String indexUserDescription( final IndexDescriptor descriptor,
                                         final SchemaIndexProvider.Descriptor providerDescriptor )
    {
        String userDescription = descriptor.userDescription( tokenNameLookup );
        return String.format( "%s [provider: %s]", userDescription, providerDescriptor.toString() );
    }

    public ValidatedIndexUpdates validate( IndexUpdates updates )
    {
        IndexUpdateMode updateMode = null;
        if ( state == State.RUNNING )
        {
            updateMode = IndexUpdateMode.ONLINE;
        }
        else if ( state == State.STARTING )
        {
            updateMode = IndexUpdateMode.RECOVERY;
        }

        if ( updateMode != null )
        {
            IndexUpdaterMap updaterMap = indexMapReference.getIndexUpdaterMap( updateMode );
            boolean updaterMapShouldBeClosed = true;
            try
            {
                Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex = groupUpdatesByIndexDescriptor(
                        updates, updaterMap );

                if ( updatesByIndex.isEmpty() )
                {
                    return newValidatedIndexUpdates( updates );
                }

                AggregatedReservation aggregatedReservation = new AggregatedReservation( updatesByIndex.size() );
                for ( Map.Entry<IndexDescriptor,List<NodePropertyUpdate>> entry : updatesByIndex.entrySet() )
                {
                    validateAndRecordReservation( entry.getValue(), aggregatedReservation, updaterMap,
                            entry.getKey() );
                }

                ValidatedIndexUpdates validatedUpdates = newValidatedIndexUpdates( updates, updaterMap, updatesByIndex,
                        aggregatedReservation );

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
        return newValidatedIndexUpdates( updates );
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

    private static ValidatedIndexUpdates newValidatedIndexUpdates( final IndexUpdates indexUpdates )
    {
        return new ValidatedIndexUpdates()
        {
            @Override
            public void flush() throws IOException, IndexEntryConflictException
            {
            }

            @Override
            public Set<Long> changedNodeIds()
            {
                return indexUpdates.changedNodeIds();
            }

            @Override
            public void close()
            {
            }
        };
    }

    private static ValidatedIndexUpdates newValidatedIndexUpdates( final IndexUpdates indexUpdates,
            final IndexUpdaterMap indexUpdaters, final Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex,
            final Reservation reservation )
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
            public Set<Long> changedNodeIds()
            {
                return indexUpdates.changedNodeIds();
            }

            @Override
            public void close()
            {
                reservation.release();
                indexUpdaters.close();
            }
        };
    }

    public void updateIndexes( ValidatedIndexUpdates updates )
    {
        if ( state == State.RUNNING )
        {
            try
            {
                updates.flush();
            }
            catch ( IOException | IndexEntryConflictException | IndexCapacityExceededException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
        else
        {
            if ( state == State.NOT_STARTED )
            {
                recoveredNodeIds.addAll( updates.changedNodeIds() );
            }
            else
            {
                // This is a temporary measure to resolve a corruption bug. We believe that it's caused by stray
                // HA transactions, and we know that this measure will fix it. It appears, however, that the correct
                // fix will be, as it is for several other issues, to modify the system to allow us to kill running
                // transactions before state switches.
                throw new IllegalStateException( "Cannot queue index updates while index service is " + state );
            }
        }
    }

    protected void applyRecoveredUpdates() throws IOException
    {
        logger.debug( "Applying recovered updates: " + recoveredNodeIds );
        monitor.applyingRecoveredData( recoveredNodeIds );
        if ( !recoveredNodeIds.isEmpty() )
        {
            try ( IndexUpdaterMap updaterMap = indexMapReference.getIndexUpdaterMap( IndexUpdateMode.RECOVERY ) )
            {
                for ( IndexUpdater updater : updaterMap )
                {
                    updater.remove( recoveredNodeIds );
                }

                List<NodePropertyUpdate> recoveredUpdates = new ArrayList<>();
                for ( long nodeId : recoveredNodeIds )
                {
                    Iterables.addAll( recoveredUpdates, storeView.nodeAsUpdates( nodeId ) );
                }

                try ( ValidatedIndexUpdates validatedUpdates = validate( asIndexUpdates( recoveredUpdates ) ) )
                {
                    validatedUpdates.flush();
                    monitor.appliedRecoveredData( recoveredUpdates );
                }
                catch ( IndexEntryConflictException | IndexCapacityExceededException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }
        }
        recoveredNodeIds.clear();
    }

    private static IndexUpdates asIndexUpdates( final Iterable<NodePropertyUpdate> updates )
    {
        return new IndexUpdates()
        {
            @Override
            public Set<Long> changedNodeIds()
            {
                return Collections.emptySet();
            }

            @Override
            public Iterator<NodePropertyUpdate> iterator()
            {
                return updates.iterator();
            }
        };
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
                    int labelAfter = update.getLabelAfter( i );
                    storeUpdateIfIndexExists( updaterMap, update, propertyKeyId, labelAfter, updatesByIndex );
                }
                break;

            case REMOVED:
                for ( int len = update.getNumberOfLabelsBefore(), i = 0; i < len; i++ )
                {
                    int labelBefore = update.getLabelBefore( i );
                    storeUpdateIfIndexExists( updaterMap, update, propertyKeyId, labelBefore, updatesByIndex );
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
                        storeUpdateIfIndexExists( updaterMap, update, propertyKeyId, labelAfter, updatesByIndex );
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
            int propertyKeyId, int labelId, Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex )
    {
        IndexDescriptor descriptor = new IndexDescriptor( labelId, propertyKeyId );
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
        IndexProxy index = indexMapReference.removeIndexProxy( indexId );
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

    private IndexProxy createAndStartPopulatingIndexProxy( final long ruleId,
                                                           final IndexDescriptor descriptor,
                                                           final SchemaIndexProvider.Descriptor providerDescriptor,
                                                           final boolean constraint ) throws IOException
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        // TODO: This is here because there is a circular dependency from PopulatingIndexProxy to FlippableIndexProxy
        final String indexUserDescription = indexUserDescription( descriptor, providerDescriptor );
        IndexPopulator populator =
            getPopulatorFromProvider( providerDescriptor, ruleId, descriptor, new IndexConfiguration( constraint ) );

        FailedIndexProxyFactory failureDelegateFactory =
            new FailedPopulatingIndexProxyFactory( descriptor, providerDescriptor, populator, indexUserDescription,
                    logger );

        PopulatingIndexProxy populatingIndex =
            new PopulatingIndexProxy( scheduler, descriptor, providerDescriptor,
                    failureDelegateFactory, populator, flipper, storeView,
                indexUserDescription, updateableSchemaState, logging, monitor );
        flipper.flipTo( populatingIndex );

        // Prepare for flipping to online mode
        flipper.setFlipTarget( new IndexProxyFactory()
        {
            @Override
            public IndexProxy create()
            {
                try
                {
                    OnlineIndexProxy onlineProxy = new OnlineIndexProxy(
                            descriptor, providerDescriptor,
                            getOnlineAccessorFromProvider( providerDescriptor, ruleId,
                                                           new IndexConfiguration( constraint ) ) );
                    if ( constraint )
                    {
                        return new TentativeConstraintIndexProxy( flipper, onlineProxy );
                    }
                    return onlineProxy;
                }
                catch ( IOException e )
                {
                    return
                        createAndStartFailedIndexProxy( ruleId, descriptor, providerDescriptor, constraint, failure( e ) );
                }
            }
        } );

        IndexProxy result = contractCheckedProxy( flipper, false );
        result.start();
        return result;
    }

    private IndexProxy createAndStartOnlineIndexProxy( long ruleId,
                                                       IndexDescriptor descriptor,
                                                       SchemaIndexProvider.Descriptor providerDescriptor,
                                                       boolean unique )
    {
        // TODO Hook in version verification/migration calls to the SchemaIndexProvider here
        try
        {
            IndexAccessor onlineAccessor = getOnlineAccessorFromProvider( providerDescriptor, ruleId,
                                                                          new IndexConfiguration( unique ) );
            IndexProxy result = new OnlineIndexProxy( descriptor, providerDescriptor, onlineAccessor );
            result = contractCheckedProxy( result, true );
            return result;
        }
        catch ( IOException e )
        {
            return createAndStartFailedIndexProxy( ruleId, descriptor, providerDescriptor, unique, failure( e ) );
        }
    }

    private IndexProxy createAndStartFailedIndexProxy( long ruleId,
                                                       IndexDescriptor descriptor,
                                                       SchemaIndexProvider.Descriptor providerDescriptor,
                                                       boolean unique,
                                                       IndexPopulationFailure populationFailure )
    {
        IndexPopulator indexPopulator = getPopulatorFromProvider( providerDescriptor, ruleId,
                descriptor, new IndexConfiguration( unique ) );
        String indexUserDescription = indexUserDescription(descriptor, providerDescriptor);
        IndexProxy result =
            new FailedIndexProxy( descriptor, providerDescriptor, indexUserDescription,
                                  indexPopulator, populationFailure, logger );
        result = contractCheckedProxy( result, true );
        return result;
    }

    private IndexProxy createAndStartRecoveringIndexProxy( IndexDescriptor descriptor,
                                                           SchemaIndexProvider.Descriptor providerDescriptor )
    {
        IndexProxy result = new RecoveringIndexProxy( descriptor, providerDescriptor );
        result = contractCheckedProxy( result, true );
        return result;
    }

    private IndexPopulator getPopulatorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor, long ruleId,
                                                     IndexDescriptor descriptor, IndexConfiguration config )
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getPopulator( ruleId, descriptor, config );
    }

    private IndexAccessor getOnlineAccessorFromProvider( SchemaIndexProvider.Descriptor providerDescriptor,
                                                         long ruleId, IndexConfiguration config ) throws IOException
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getOnlineAccessor( ruleId, config );
    }

    private IndexProxy contractCheckedProxy( IndexProxy result, boolean started )
    {
        result = new ContractCheckingIndexProxy( result, started );
        return result;
    }

    private IndexDescriptor createDescriptor( IndexRule rule )
    {
        return new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
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

    private void dropRecoveringIndexes(
        IndexMap indexMap, Map<Long, Pair<IndexDescriptor,SchemaIndexProvider.Descriptor>> recoveringIndexes )
            throws IOException
    {
        for ( long indexId : recoveringIndexes.keySet() )
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
                IndexProxy index = getProxyForRule( indexId );
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

    public void validateIndex( long indexId ) throws IndexNotFoundKernelException, ConstraintVerificationFailedKernelException, IndexPopulationFailedKernelException
    {
        getProxyForRule( indexId ).validate();
    }

    public void flushAll()
    {
        for ( IndexProxy index : indexMapReference.getAllIndexProxies() )
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

    private void closeAllIndexes()
    {
        Iterable<IndexProxy> indexesToStop = indexMapReference.clear();
        Collection<Future<Void>> indexStopFutures = new ArrayList<>();
        for ( IndexProxy index : indexesToStop )
        {
            try
            {
                indexStopFutures.add( index.close() );
            }
            catch ( IOException e )
            {
                logger.error( "Unable to close index", e );
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
                logger.error( "Error awaiting index to close", e );
            }
        }
    }

    private Pair<IndexDescriptor, SchemaIndexProvider.Descriptor> getIndexProxyDescriptors( IndexProxy indexProxy )
    {
        return Pair.of( indexProxy.getDescriptor(), indexProxy.getProviderDescriptor() );
    }

    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        Set<SchemaIndexProvider.Descriptor> fromProviders = new HashSet<>();
        for ( IndexProxy indexProxy : indexMapReference.getAllIndexProxies() )
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
}
