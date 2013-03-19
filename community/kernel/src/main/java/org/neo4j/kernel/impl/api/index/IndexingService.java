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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.helpers.Exceptions.launderedException;

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
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

/**
 * Manages the "schema indexes" that were introduced in 2.0. These indexes depend on the normal neo4j logical log
 * for transactionality. Each index has an {@link IndexRule}, which it uses to filter changes that come into the
 * database. Changes that apply to the the rule are indexed. This way, "normal" changes to the database can be
 * replayed to perform recovery after a crash.
 *
 * <h3>Recovery procedure</h3>
 *
 * Each index has a state, as defined in {@link org.neo4j.kernel.api.index.InternalIndexState}, which is used during recovery. If
 * an index is anything but {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, it will simply be destroyed and re-created.
 *
 * If, however, it is {@link org.neo4j.kernel.api.index.InternalIndexState#ONLINE}, the index provider is required to also guarantee
 * that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter
{
    // TODO create hierarchy of filters for smarter update processing

    private final JobScheduler scheduler;
    private final SchemaIndexProvider provider;

    private final ConcurrentHashMap<Long, IndexProxy> indexes = new ConcurrentHashMap<Long, IndexProxy>();
    private boolean serviceRunning = false;
    private final IndexStoreView storeView;
    private final Logging logging;
    private final StringLogger logger;

    public IndexingService( JobScheduler scheduler, SchemaIndexProvider provider,
            IndexStoreView storeView, Logging logging )
    {
        this.scheduler = scheduler;
        this.provider = provider;
        this.storeView = storeView;
        this.logging = logging;
        this.logger = logging.getLogger( getClass() );

        if ( provider == null )
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
        Map<Long, IndexDescriptor> rebuildingIndexDescriptors = new HashMap<Long, IndexDescriptor>();

        // Find all indexes that are not already online, do not require rebuilding, and create them
        for ( Map.Entry<Long, IndexProxy> entry : indexes.entrySet() )
        {
            long ruleId = entry.getKey();
            IndexProxy indexProxy = entry.getValue();
            switch ( indexProxy.getState() )
            {
            case ONLINE:
                // Don't do anything, index is ok.
                break;
            case POPULATING:
                // Remember for rebuilding
                rebuildingIndexes.add( indexProxy );
                rebuildingIndexDescriptors.put( ruleId, indexProxy.getDescriptor() );
                break;
            case FAILED:
                // Don't do anything, the user needs to drop the index and re-create
                break;
            }
        }

        // Drop placeholder proxies for indexes that need to be rebuilt
        dropIndexes( rebuildingIndexes );

        // Rebuild indexes by recreating and repopulating them
        for ( Map.Entry<Long, IndexDescriptor> entry : rebuildingIndexDescriptors.entrySet() )
        {
            long ruleId = entry.getKey();
            IndexDescriptor descriptor = entry.getValue();
            IndexProxy indexProxy = createPopulatingIndexProxy( ruleId, descriptor );
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

    public IndexDescriptor getIndexDescriptor( long indexId ) throws IndexNotFoundKernelException
    {
        return getProxyForRule( indexId ).getDescriptor();
    }

    /**
     * Called while the database starts up, before recovery.
     *
     * @param indexRules Known index rules before recovery.
     */
    public void initIndexes( Iterable<IndexRule> indexRules )
    {
        for ( IndexRule indexRule : indexRules )
        {
            long ruleId = indexRule.getId();
            IndexDescriptor descriptor = createDescriptor( indexRule );
            IndexProxy indexProxy = null;
            switch ( provider.getInitialState( ruleId ) )
            {
                case ONLINE:
                    indexProxy = createOnlineIndexProxy( ruleId, descriptor );
                    break;
                case POPULATING:
                    // The database was shut down during population, or a crash has occurred, or some other
                    // sad thing.
                    indexProxy = createRecoveringIndexProxy( ruleId, descriptor );
                    break;
                case FAILED:
                    indexProxy = createFailedIndexProxy( ruleId, descriptor );
                    break;
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
        IndexProxy index = indexes.get( rule.getId() );
        long ruleId = rule.getId();
        IndexDescriptor descriptor = createDescriptor( rule );
        if ( serviceRunning )
        {
            assert index == null : "Index " + rule + " already exists";
            index = createPopulatingIndexProxy( ruleId, descriptor );
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
            index = createRecoveringIndexProxy( ruleId, descriptor );
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

    private IndexProxy createPopulatingIndexProxy( final long ruleId, final IndexDescriptor descriptor )
    {
        FlippableIndexProxy flippableIndex = new FlippableIndexProxy( );

        // TODO: This is here because there is a circular dependency from PopulatingIndexProxy to FlippableIndexProxy
        IndexPopulator populator = getPopulatorFromProvider( ruleId );
        PopulatingIndexProxy populatingIndex =
                new PopulatingIndexProxy( scheduler, descriptor, populator, flippableIndex, storeView, logging );
        flippableIndex.setFlipTarget( singleProxy( populatingIndex ) );
        flippableIndex.flip();

        // Prepare for flipping to online mode
        flippableIndex.setFlipTarget( new IndexProxyFactory()
        {
            @Override
            public IndexProxy create()
            {
                return new OnlineIndexProxy( descriptor, getOnlineAccessorFromProvider( ruleId ) );
            }
        } );

        IndexProxy result = contractCheckedProxy( flippableIndex, false );
        return serviceDecoratedProxy( ruleId, result );
    }

    private IndexProxy createOnlineIndexProxy( long ruleId, IndexDescriptor descriptor )
    {
        IndexAccessor onlineAccessor = getOnlineAccessorFromProvider( ruleId );
        IndexProxy result = new OnlineIndexProxy( descriptor, onlineAccessor );
        result = contractCheckedProxy( result, true );
        result = serviceDecoratedProxy( ruleId, result );
        return result;
    }

    private IndexProxy createFailedIndexProxy( long ruleId, IndexDescriptor descriptor )
    {
        IndexProxy result = new FailedIndexProxy( descriptor, getPopulatorFromProvider( ruleId ) );
        result = contractCheckedProxy( result, true );
        return serviceDecoratedProxy( ruleId, result );
    }

    private IndexProxy createRecoveringIndexProxy( long ruleId, IndexDescriptor descriptor )
    {
        IndexProxy result = new RecoveringIndexProxy( descriptor );
        result = contractCheckedProxy( result, true );
        return serviceDecoratedProxy( ruleId, result );
    }

    private IndexPopulator getPopulatorFromProvider( long ruleId )
    {
        return provider.getPopulator( ruleId );
    }

    private IndexAccessor getOnlineAccessorFromProvider( long ruleId  )
    {
        return provider.getOnlineAccessor( ruleId );
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

    public static IndexProxyFactory singleProxy( final IndexProxy proxy )
    {
        return new IndexProxyFactory()
        {
            @Override
            public IndexProxy create()
            {
                return proxy;
            }
        };
    }
    
    public interface StoreScan
    {
        void run();
        
        void stop();
    }
    
    /**
     * The indexing services view of the universe.
     */
    public interface IndexStoreView
    {
        /**
         * Get properties of a node, if those properties exist.
         * @param nodeId
         * @param propertyKeys
         */
        Iterator<Pair<Integer, Object>> getNodeProperties( long nodeId, Iterator<Long> propertyKeys );

        /**
         * Retrieve all nodes in the database with a given label and property, as pairs of node id and
         * property value.
         *
         * @return a {@link StoreScan} to start and to stop the scan.
         */
        StoreScan visitNodesWithPropertyAndLabel( IndexDescriptor descriptor, Visitor<Pair<Long, Object>> visitor );
    }
}
