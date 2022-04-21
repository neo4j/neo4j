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

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class DynamicIndexStoreView implements IndexStoreView {
    private final FullScanStoreView fullScanStoreView;
    private final Locks locks;
    protected final LockService lockService;
    private final Config config;
    private final IndexProxyProvider indexProxies;
    protected final Supplier<StorageReader> storageReader;
    private final Function<CursorContext, StoreCursors> cursorFactory;
    private final InternalLog log;

    public DynamicIndexStoreView(
            FullScanStoreView fullScanStoreView,
            Locks locks,
            LockService lockService,
            Config config,
            IndexProxyProvider indexProxies,
            Supplier<StorageReader> storageReader,
            Function<CursorContext, StoreCursors> cursorFactory,
            InternalLogProvider logProvider) {
        this.fullScanStoreView = fullScanStoreView;
        this.locks = locks;
        this.lockService = lockService;
        this.config = config;
        this.indexProxies = indexProxies;
        this.storageReader = storageReader;
        this.cursorFactory = cursorFactory;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public StoreScan visitNodes(
            int[] labelIds,
            IntPredicate propertyKeyIdFilter,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer labelScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        var tokenIndex = findTokenIndex(storageReader, NODE);
        if (tokenIndex.isPresent()) {
            var nodeStoreScan = new LabelIndexedNodeStoreScan(
                    config,
                    storageReader.get(),
                    cursorFactory,
                    lockService,
                    tokenIndex.get().reader,
                    labelScanConsumer,
                    propertyScanConsumer,
                    labelIds,
                    propertyKeyIdFilter,
                    parallelWrite,
                    fullScanStoreView.scheduler,
                    contextFactory,
                    memoryTracker);
            return new IndexedStoreScan(locks, tokenIndex.get().descriptor, config, nodeStoreScan);
        }

        return fullScanStoreView.visitNodes(
                labelIds,
                propertyKeyIdFilter,
                propertyScanConsumer,
                labelScanConsumer,
                forceStoreScan,
                parallelWrite,
                contextFactory,
                memoryTracker);
    }

    @Override
    public StoreScan visitRelationships(
            int[] relationshipTypeIds,
            IntPredicate propertyKeyIdFilter,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer relationshipTypeScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {

        var tokenIndex = findTokenIndex(storageReader, RELATIONSHIP);
        if (tokenIndex.isPresent()) {
            var relationshipStoreScan = new RelationshipIndexedRelationshipStoreScan(
                    config,
                    storageReader.get(),
                    cursorFactory,
                    lockService,
                    tokenIndex.get().reader,
                    relationshipTypeScanConsumer,
                    propertyScanConsumer,
                    relationshipTypeIds,
                    propertyKeyIdFilter,
                    parallelWrite,
                    fullScanStoreView.scheduler,
                    contextFactory,
                    memoryTracker);
            return new IndexedStoreScan(locks, tokenIndex.get().descriptor, config, relationshipStoreScan);
        }

        return fullScanStoreView.visitRelationships(
                relationshipTypeIds,
                propertyKeyIdFilter,
                propertyScanConsumer,
                relationshipTypeScanConsumer,
                forceStoreScan,
                parallelWrite,
                contextFactory,
                memoryTracker);
    }

    @Override
    public boolean isEmpty(CursorContext cursorContext) {
        return fullScanStoreView.isEmpty(cursorContext);
    }

    private Optional<TokenIndexData> findTokenIndex(Supplier<StorageReader> storageReader, EntityType entityType) {
        var descriptor = storageReader
                .get()
                .indexGetForSchemaAndType(SchemaDescriptors.forAnyEntityTokens(entityType), IndexType.LOOKUP);
        if (descriptor == null) {
            return Optional.empty();
        }
        try {
            IndexProxy indexProxy = indexProxies.getIndexProxy(descriptor);
            if (indexProxy.getState() == InternalIndexState.ONLINE) {
                return Optional.of(new TokenIndexData(indexProxy.newTokenReader(), indexProxy.getDescriptor()));
            }
        } catch (IndexNotFoundKernelException e) {
            log.warn("Token index missing for entity: %s, switching to full scan", entityType, e);
        }
        return Optional.empty();
    }

    private record TokenIndexData(TokenIndexReader reader, IndexDescriptor descriptor) {}
}
