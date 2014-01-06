/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.BiConsumer;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
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

    enum State
    {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    private volatile State state = State.NOT_STARTED;

    public IndexingService( JobScheduler scheduler,
                            SchemaIndexProviderMap providerMap,
                            IndexStoreView storeView,
                            TokenNameLookup tokenNameLookup,
                            UpdateableSchemaState updateableSchemaState,
                            Logging logging )
    {
        this.scheduler = scheduler;
        this.providerMap = providerMap;
        this.storeView = storeView;
        this.logging = logging;
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
    @Override
    public void start() throws Exception
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
        if (index != null)
        {
            // We already have this index
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

    public void updateIndexes( IndexUpdates updates )
    {
        if ( state == State.RUNNING )
        {
            try ( IndexUpdaterMap updaterMap = indexMapReference.getIndexUpdaterMap( IndexUpdateMode.ONLINE ) )
            {
                applyUpdates( updates, updaterMap );
            }
        }
        else
        {
            if( state == State.NOT_STARTED )
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

    private void applyRecoveredUpdates() throws IOException
    {
        try ( IndexUpdaterMap updaterMap = indexMapReference.getIndexUpdaterMap( IndexUpdateMode.RECOVERY ) )
        {
            if ( !recoveredNodeIds.isEmpty() )
            {
                for ( IndexUpdater updater : updaterMap )
                {
                    updater.remove( recoveredNodeIds );
                }
                for ( long nodeId : recoveredNodeIds )
                {
                    applyUpdates( storeView.nodeAsUpdates( nodeId ), updaterMap );
                }
            }
        }
        recoveredNodeIds.clear();
    }

    private void applyUpdates( Iterable<NodePropertyUpdate> updates, IndexUpdaterMap updaterMap )
    {
        for ( NodePropertyUpdate update : updates )
        {
            int propertyKeyId = update.getPropertyKeyId();
            switch (update.getUpdateMode())
            {
            case ADDED:
                for ( int len = update.getNumberOfLabelsAfter(), i = 0; i < len; i++ )
                {
                    processUpdateIfIndexExists( updaterMap, update, propertyKeyId, update.getLabelAfter( i ) );
                }
                break;

            case REMOVED:
                for ( int len = update.getNumberOfLabelsBefore(), i = 0; i < len; i++ )
                {
                    processUpdateIfIndexExists( updaterMap, update, propertyKeyId, update.getLabelBefore( i ) );
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
                        processUpdateIfIndexExists( updaterMap, update, propertyKeyId, labelAfter );
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
    }

    private void processUpdateIfIndexExists( IndexUpdaterMap updaterMap, NodePropertyUpdate update,
                                             int propertyKeyId, int labelId )
    {
        IndexDescriptor descriptor = new IndexDescriptor( labelId, propertyKeyId );
        try
        {
            IndexUpdater updater = updaterMap.getUpdater( descriptor );
            if ( null != updater )
            {
                updater.process( update );
            }
        }
        catch ( IOException | IndexEntryConflictException e )
        {
            throw new UnderlyingStorageException( e );
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
            getPopulatorFromProvider( providerDescriptor, ruleId, new IndexConfiguration( constraint ) );

        FailedIndexProxyFactory failureDelegateFactory =
            new FailedPopulatingIndexProxyFactory( descriptor, providerDescriptor, populator, indexUserDescription );

        PopulatingIndexProxy populatingIndex =
            new PopulatingIndexProxy( scheduler, descriptor, providerDescriptor,
                    failureDelegateFactory, populator, flipper, storeView,
                indexUserDescription, updateableSchemaState, logging );
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
                                                                  new IndexConfiguration( unique ) );
        String indexUserDescription = indexUserDescription(descriptor, providerDescriptor);
        IndexProxy result =
            new FailedIndexProxy( descriptor, providerDescriptor, indexUserDescription,
                                  indexPopulator, populationFailure );
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
                                                     IndexConfiguration config )
    {
        SchemaIndexProvider indexProvider = providerMap.apply( providerDescriptor );
        return indexProvider.getPopulator( ruleId, config );
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
            throws Exception
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
        IndexProxy index = getProxyForRule( indexId );
        try
        {
            if ( state == State.RUNNING ) // don't do this during recovery.
            {
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
        for ( IndexProxy indexProxy : indexMapReference.getAllIndexProxies() )
        {
            snapshots.add(indexProxy.snapshotFiles());
        }

        return concatResourceIterators( snapshots.iterator() );
    }
}

