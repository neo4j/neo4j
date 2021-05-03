/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

public class DynamicIndexStoreView implements IndexStoreView
{
    private final FullScanStoreView fullScanStoreView;
    private final Locks locks;
    private final LockService lockService;
    private final Config config;
    private final IndexProxyProvider indexProxies;
    protected final Supplier<StorageReader> storageReader;
    private final Log log;

    public DynamicIndexStoreView( FullScanStoreView fullScanStoreView,
                                  Locks locks,
                                  LockService lockService,
                                  Config config,
                                  IndexProxyProvider indexProxies,
                                  Supplier<StorageReader> storageReader,
                                  LogProvider logProvider )
    {
        this.fullScanStoreView = fullScanStoreView;
        this.locks = locks;
        this.lockService = lockService;
        this.config = config;
        this.indexProxies = indexProxies;
        this.storageReader = storageReader;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public StoreScan visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter,
                                 PropertyScanConsumer propertyScanConsumer, TokenScanConsumer labelScanConsumer,
                                 boolean forceStoreScan, boolean parallelWrite, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        var tokenIndex = findTokenIndex( storageReader, NODE );
        if ( tokenIndex.isPresent() )
        {
            var nodeStoreScan = new LabelIndexedNodeStoreScan(
                    config, storageReader.get(), lockService, tokenIndex.get().reader, labelScanConsumer, propertyScanConsumer, labelIds,
                    propertyKeyIdFilter, parallelWrite, fullScanStoreView.scheduler, cacheTracer, memoryTracker );
            return new IndexedStoreScan( locks, tokenIndex.get().descriptor, config, nodeStoreScan );
        }

        return fullScanStoreView.visitNodes(
                labelIds, propertyKeyIdFilter, propertyScanConsumer, labelScanConsumer, forceStoreScan, parallelWrite, cacheTracer, memoryTracker );
    }

    @Override
    public StoreScan visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter, PropertyScanConsumer propertyScanConsumer,
                                         TokenScanConsumer relationshipTypeScanConsumer, boolean forceStoreScan, boolean parallelWrite,
                                         PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {

        var tokenIndex = findTokenIndex( storageReader, RELATIONSHIP );
        if ( tokenIndex.isPresent() )
        {
            var relationshipStoreScan = new RelationshipIndexedRelationshipStoreScan(
                    config, storageReader.get(), lockService, tokenIndex.get().reader, relationshipTypeScanConsumer, propertyScanConsumer, relationshipTypeIds,
                    propertyKeyIdFilter, parallelWrite, fullScanStoreView.scheduler, cacheTracer, memoryTracker );
            return new IndexedStoreScan( locks, tokenIndex.get().descriptor, config, relationshipStoreScan );
        }

        return fullScanStoreView.visitRelationships(
                relationshipTypeIds, propertyKeyIdFilter, propertyScanConsumer,
                relationshipTypeScanConsumer, forceStoreScan, parallelWrite, cacheTracer, memoryTracker );
    }

    @Override
    public boolean isEmpty()
    {
        return fullScanStoreView.isEmpty();
    }

    @Override
    public NodePropertyAccessor newPropertyAccessor( CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return fullScanStoreView.newPropertyAccessor( cursorContext, memoryTracker );
    }

    private Optional<TokenIndexData> findTokenIndex( Supplier<StorageReader> storageReader, EntityType entityType )
    {
        Iterator<IndexDescriptor> descriptorIterator = storageReader.get().indexGetForSchema( SchemaDescriptor.forAnyEntityTokens( entityType ) );
        if ( !descriptorIterator.hasNext() )
        {
            return Optional.empty();
        }
        try
        {
            IndexProxy indexProxy = indexProxies.getIndexProxy( descriptorIterator.next() );
            if ( indexProxy.getState() == InternalIndexState.ONLINE )
            {
                return Optional.of( new TokenIndexData( indexProxy.newTokenReader(), indexProxy.getDescriptor() ) );
            }
        }
        catch ( IndexNotFoundKernelException e )
        {
            log.warn( "Token index missing for entity: %s, switching to full scan", entityType, e );
        }
        return Optional.empty();
    }

    private static class TokenIndexData
    {
        private final TokenIndexReader reader;
        private final IndexDescriptor descriptor;

        private TokenIndexData( TokenIndexReader reader, IndexDescriptor descriptor )
        {
            this.reader = reader;
            this.descriptor = descriptor;
        }
    }
}
