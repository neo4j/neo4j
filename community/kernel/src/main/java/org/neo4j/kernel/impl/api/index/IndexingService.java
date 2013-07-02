/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
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
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

/**
 * Manages the "schema indexes" that were introduced in 2.0. These indexes depend on the normal neo4j logical log for
 * transactionality. Each index has an {@link org.neo4j.kernel.impl.nioneo.store.IndexRule}, which it uses to filter changes that come into the database.
 * Changes that apply to the the rule are indexed. This way, "normal" changes to the database can be replayed to perform
 * recovery after a crash.
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
    // TODO create hierarchy of filters for smarter update processing

    private final JobScheduler scheduler;
    private final SchemaIndexProviderMap providerMap;

    private final ConcurrentHashMap<Long, IndexProxy> indexes = new ConcurrentHashMap<Long, IndexProxy>();
    private boolean serviceRunning = false;
    private final IndexStoreView storeView;
    private final Logging logging;
    private final StringLogger logger;
    private final UpdateableSchemaState updateableSchemaState;

    public IndexingService( JobScheduler scheduler,
                            SchemaIndexProviderMap providerMap,
                            IndexStoreView storeView,
                            UpdateableSchemaState updateableSchemaState,
                            Logging logging )
    {
        this.scheduler = scheduler;
        this.providerMap = providerMap;
        this.storeView = storeView;
        this.logging = logging;
        this.logger = logging.getMessagesLog( getClass() );
        this.updateableSchemaState = updateableSchemaState;

        if ( providerMap == null || providerMap.getDefaultProvider() == null )
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without providing a schema index provider, " +
                                             "please make sure that a valid provider is on your classpath." );
        }
    }

    // Recovery semantics: This is to be called after initIndexes, and after the database has run recovery.
    @Override
    public void start() throws Exception
    {
        Set<IndexProxy> rebuildingIndexes = new HashSet<IndexProxy>();
        Map<Long, Pair<IndexDescriptor, SchemaIndexProvider.Descriptor>> rebuildingIndexDescriptors =
                new HashMap<Long, Pair<IndexDescriptor, SchemaIndexProvider.Descriptor>>();

        // Find all indexes that are not already online, do not require rebuilding, and create them
        for ( Map.Entry<Long, IndexProxy> entry : indexes.entrySet() )
        {
            long ruleId = entry.getKey();
            IndexProxy indexProxy = entry.getValue();
            InternalIndexState state = indexProxy.getState();
            logger.info( String.format( "IndexingService.start: index on %s is %s",
                    indexProxy.getDescriptor().toString(), state.name() ) );
            switch ( state )
            {
            case ONLINE:
                // Don't do anything, index is ok.
                break;
            case POPULATING:
                // Remember for rebuilding
                rebuildingIndexes.add( indexProxy );
                Pair<IndexDescriptor, SchemaIndexProvider.Descriptor> descriptors =
                        Pair.of( indexProxy.getDescriptor(), indexProxy.getProviderDescriptor() );
                rebuildingIndexDescriptors.put( ruleId, descriptors );
                break;
            case FAILED:
                // Don't do anything, the user needs to drop the index and re-create
                break;
            }
        }

        // Drop placeholder proxies for indexes that need to be rebuilt
        dropIndexes( rebuildingIndexes );

        // Rebuild indexes by recreating and repopulating them
        for ( Map.Entry<Long, Pair<IndexDescriptor, SchemaIndexProvider.Descriptor>> entry : rebuildingIndexDescriptors.entrySet() )
        {
            long ruleId = entry.getKey();
            Pair<IndexDescriptor, SchemaIndexProvider.Descriptor> descriptors = entry.getValue();
            IndexDescriptor indexDescriptor = descriptors.first();
            SchemaIndexProvider.Descriptor providerDescriptor = descriptors.other();
            IndexProxy indexProxy = createPopulatingIndexProxy( ruleId, indexDescriptor, providerDescriptor,
                                                                serviceRunning );
            indexProxy.start();
            indexes.put( ruleId, indexProxy );
        }

        serviceRunning = true;
    }

    @Override
    public void stop()
    {
        serviceRunning = false;
        closeAllIndexes();
    }

    private void closeAllIndexes()
    {
        Collection<IndexProxy> indexesToStop = new ArrayList<IndexProxy>( indexes.values() );
        indexes.clear();
        Collection<Future<Void>> indexStopFutures = new ArrayList<Future<Void>>();
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

    public IndexProxy getProxyForRule( long indexId ) throws IndexNotFoundKernelException
    {
        IndexProxy indexProxy = indexes.get( indexId );
        if ( indexProxy == null )
        {
            throw new IndexNotFoundKernelException( "No index with id " + indexId + " exists." );
        }
        return indexProxy;
    }

    /**
     * Called while the database starts up, before recovery.
     *
     * @param indexRules Known index rules before recovery.
     */
    public void initIndexes( Iterator<IndexRule> indexRules )
    {
        for ( IndexRule indexRule : loop( indexRules ) )
        {
            long ruleId = indexRule.getId();
            IndexDescriptor descriptor = createDescriptor( indexRule );
            IndexProxy indexProxy = null;
            SchemaIndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
            SchemaIndexProvider provider = providerMap.apply( providerDescriptor );
            InternalIndexState initialState = provider.getInitialState( ruleId );
            logger.info( format( "IndexingService.initIndexes: index on %s is %s",
                    descriptor.toString(), initialState.name() ) );
            switch ( initialState )
            {
            case ONLINE:
                // TODO ask provider to verify
                indexProxy = createOnlineIndexProxy( ruleId, descriptor, providerDescriptor, indexRule.isConstraintIndex() );
                break;
            case POPULATING:
                // The database was shut down during population, or a crash has occurred, or some other sad thing.
                indexProxy = createRecoveringIndexProxy( ruleId, descriptor, providerDescriptor );
                break;
            case FAILED:
                indexProxy = createFailedIndexProxy( ruleId, descriptor, providerDescriptor, indexRule.isConstraintIndex(),
                                                     failure( provider.getPopulationFailure( ruleId ) ) );
                break;
            default:
                throw new IllegalArgumentException( "" + initialState );
            }
            indexes.put( ruleId, indexProxy );
        }
    }

    /*
     * Creates a new index.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     */
    public void createIndex( IndexRule rule )
    {
        long ruleId = rule.getId();
        IndexProxy index = indexes.get( ruleId );
        IndexDescriptor descriptor = createDescriptor( rule );
        if ( serviceRunning )
        {
            assert index == null : "Index " + rule + " already exists";
            index = createPopulatingIndexProxy( ruleId, descriptor, rule.getProviderDescriptor(), rule.isConstraintIndex() );
            try
            {
                index.start();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else if ( index == null )
        {
            index = createRecoveringIndexProxy( ruleId, descriptor, rule.getProviderDescriptor() );
        }

        indexes.put( rule.getId(), index );
    }

    public void updateIndexes( Iterable<NodePropertyUpdate> updates )
    {
        if ( serviceRunning )
        {
            for ( IndexProxy index : indexes.values() )
            {
                try
                {
                    index.update( updates );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( "Unable to update " + index, e );
                }
            }
        }
        else
        {
            for ( IndexProxy index : indexes.values() )
            {
                try
                {
                    index.recover( updates );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( "Unable to update " + index, e );
                }
            }
        }
    }

    public void dropIndex( IndexRule rule )
    {
        IndexProxy index = indexes.remove( rule.getId() );
        if ( serviceRunning )
        {
            assert index != null : "Index " + rule + " doesn't exists";
            try
            {
                awaitIndexFuture( index.drop() );
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
    }

    private IndexProxy createPopulatingIndexProxy( final long ruleId,
                                                   final IndexDescriptor descriptor,
                                                   final SchemaIndexProvider.Descriptor providerDescriptor,
                                                   final boolean unique )
    {
        final FlippableIndexProxy flipper = new FlippableIndexProxy();

        // TODO: This is here because there is a circular dependency from PopulatingIndexProxy to FlippableIndexProxy
        IndexPopulator populator = getPopulatorFromProvider( providerDescriptor, ruleId, new IndexConfiguration( unique ) );
        PopulatingIndexProxy populatingIndex =
                new PopulatingIndexProxy( scheduler, descriptor, providerDescriptor,
                                          populator, flipper, storeView, updateableSchemaState, logging );
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
                                                           new IndexConfiguration( unique ) ) );
                    if ( unique )
                    {
                        return new TentativeConstraintIndexProxy( flipper, onlineProxy );
                    }
                    return onlineProxy;
                }
                catch ( IOException e )
                {
                    return createFailedIndexProxy( ruleId, descriptor, providerDescriptor, unique, failure( e ) );
                }
            }
        } );

        IndexProxy result = contractCheckedProxy( flipper, false );
        return serviceDecoratedProxy( ruleId, result );
    }

    private IndexProxy createOnlineIndexProxy( long ruleId,
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
            return serviceDecoratedProxy( ruleId, result );
        }
        catch ( IOException e )
        {
            return createFailedIndexProxy( ruleId, descriptor, providerDescriptor, unique, failure( e ) );
        }
    }

    private IndexProxy createFailedIndexProxy( long ruleId,
                                               IndexDescriptor descriptor,
                                               SchemaIndexProvider.Descriptor providerDescriptor, boolean unique,
                                               IndexPopulationFailure populationFailure )
    {
        IndexPopulator indexPopulator = getPopulatorFromProvider( providerDescriptor, ruleId,
                                                                  new IndexConfiguration( unique ) );
        IndexProxy result = new FailedIndexProxy( descriptor, providerDescriptor, indexPopulator,
                populationFailure );
        result = contractCheckedProxy( result, true );
        return serviceDecoratedProxy( ruleId, result );
    }

    private IndexProxy createRecoveringIndexProxy( long ruleId,
                                                   IndexDescriptor descriptor,
                                                   SchemaIndexProvider.Descriptor providerDescriptor )
    {
        IndexProxy result = new RecoveringIndexProxy( descriptor, providerDescriptor );
        result = contractCheckedProxy( result, true );
        return serviceDecoratedProxy( ruleId, result );
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

    private IndexProxy serviceDecoratedProxy( long ruleId, IndexProxy result )
    {
        // TODO: Merge auto removing and rule updating?
        result = new RuleUpdateFilterIndexProxy( result );
        result = new ServiceStateUpdatingIndexProxy( ruleId, result );
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

    private void dropIndexes( Set<IndexProxy> recoveringIndexes ) throws Exception
    {
        for ( IndexProxy indexProxy : recoveringIndexes )
        {
            indexProxy.drop().get();
        }
    }

    public void activateIndex( long indexId ) throws IndexNotFoundKernelException
    {
        getProxyForRule( indexId ).activate();
    }

    public void validateIndex( long indexId ) throws IndexNotFoundKernelException, IndexPopulationFailedKernelException
    {
        getProxyForRule( indexId ).validate();
    }

    class ServiceStateUpdatingIndexProxy extends DelegatingIndexProxy
    {
        private final long ruleId;

        ServiceStateUpdatingIndexProxy( long ruleId, IndexProxy delegate )
        {
            super( delegate );
            this.ruleId = ruleId;
        }

        @Override
        public Future<Void> drop() throws IOException
        {
            indexes.remove( ruleId, this );
            return super.drop();
        }
    }

    public void flushAll()
    {
        for ( IndexProxy index : indexes.values() )
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
}
