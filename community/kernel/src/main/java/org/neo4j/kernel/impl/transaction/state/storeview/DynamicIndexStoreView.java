/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.kernel.impl.api.KernelTransactions.SYSTEM_TRANSACTION_ID;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Optional;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageReader;

public class DynamicIndexStoreView implements IndexStoreView {
    private final FullScanStoreView fullScanStoreView;
    private final LockManager lockManager;
    protected final LockService lockService;
    private final Config config;
    private final IndexProxyProvider indexProxies;
    protected final ReadableStorageEngine storageEngine;
    private final InternalLog log;

    public DynamicIndexStoreView(
            FullScanStoreView fullScanStoreView,
            LockManager lockManager,
            LockService lockService,
            Config config,
            IndexProxyProvider indexProxies,
            ReadableStorageEngine storageEngine,
            InternalLogProvider logProvider) {
        this.fullScanStoreView = fullScanStoreView;
        this.lockManager = lockManager;
        this.lockService = lockService;
        this.config = config;
        this.indexProxies = indexProxies;
        this.storageEngine = storageEngine;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public StoreScan visitNodes(
            int[] labelIds,
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer labelScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        var tokenIndex = findTokenIndex(NODE, false);
        if (tokenIndex.isPresent()) {
            // Token index present. Lock it and check again.
            var lockClient = lockManager.newClient();
            var instantiatedIndexedScan = false;
            try {
                lockTokenIndexForScan(tokenIndex.get().descriptor(), lockClient);
                tokenIndex = findTokenIndex(NODE, true);
                if (tokenIndex.isPresent()) {
                    var nodeStoreScan = new LabelIndexedNodeStoreScan(
                            config,
                            storageEngine.newReader(),
                            storageEngine::createStorageCursors,
                            lockService,
                            tokenIndex.get().reader,
                            labelScanConsumer,
                            propertyScanConsumer,
                            labelIds,
                            propertySelection,
                            parallelWrite,
                            fullScanStoreView.scheduler,
                            contextFactory,
                            memoryTracker,
                            storageEngine.getOpenOptions().contains(PageCacheOpenOptions.MULTI_VERSIONED));
                    var indexedStoreScan = new IndexedStoreScan(lockClient, nodeStoreScan);
                    instantiatedIndexedScan = true;
                    return indexedStoreScan;
                }
            } finally {
                if (!instantiatedIndexedScan) {
                    lockClient.close();
                }
            }
        }

        return fullScanStoreView.visitNodes(
                labelIds,
                propertySelection,
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
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer relationshipTypeScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        var tokenIndex = findTokenIndex(RELATIONSHIP, false);
        if (tokenIndex.isPresent()) {
            // Token index present. Lock it and check again.
            var lockClient = lockManager.newClient();
            var instantiatedIndexedScan = false;
            try {
                lockTokenIndexForScan(tokenIndex.get().descriptor(), lockClient);
                tokenIndex = findTokenIndex(RELATIONSHIP, true);
                if (tokenIndex.isPresent()) {
                    StoreScan storeScan;
                    if (fullScanStoreView.storageEngine.indexingBehaviour().useNodeIdsInRelationshipTokenIndex()) {
                        // This index-type-lookup-index-backed relationship scan is node-based
                        storeScan = new NodeRelationshipsIndexedStoreScan(
                                config,
                                storageEngine.newReader(),
                                storageEngine::createStorageCursors,
                                lockService,
                                tokenIndex.get().reader,
                                relationshipTypeScanConsumer,
                                propertyScanConsumer,
                                relationshipTypeIds,
                                propertySelection,
                                parallelWrite,
                                fullScanStoreView.scheduler,
                                contextFactory,
                                memoryTracker);
                    } else {
                        storeScan = new RelationshipIndexedRelationshipStoreScan(
                                config,
                                storageEngine.newReader(),
                                storageEngine::createStorageCursors,
                                lockService,
                                tokenIndex.get().reader,
                                relationshipTypeScanConsumer,
                                propertyScanConsumer,
                                relationshipTypeIds,
                                propertySelection,
                                parallelWrite,
                                fullScanStoreView.scheduler,
                                contextFactory,
                                memoryTracker,
                                storageEngine.getOpenOptions().contains(PageCacheOpenOptions.MULTI_VERSIONED));
                    }
                    var indexedStoreScan = new IndexedStoreScan(lockClient, storeScan);
                    instantiatedIndexedScan = true;
                    return indexedStoreScan;
                }
            } finally {
                if (!instantiatedIndexedScan) {
                    lockClient.close();
                }
            }
        }

        return fullScanStoreView.visitRelationships(
                relationshipTypeIds,
                propertySelection,
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

    private Optional<TokenIndexData> findTokenIndex(EntityType entityType, boolean instantiateReader) {
        IndexDescriptor descriptor;
        try (StorageReader reader = storageEngine.newReader()) {
            descriptor =
                    reader.indexGetForSchemaAndType(SchemaDescriptors.forAnyEntityTokens(entityType), IndexType.LOOKUP);
        }

        if (descriptor == null) {
            return Optional.empty();
        }
        try {
            IndexProxy indexProxy = indexProxies.getIndexProxy(descriptor);
            if (indexProxy.getState() == InternalIndexState.ONLINE) {
                return Optional.of(new TokenIndexData(
                        instantiateReader ? indexProxy.newTokenReader() : null, indexProxy.getDescriptor()));
            }
        } catch (IndexNotFoundKernelException e) {
            log.warn("Token index missing for entity: %s, switching to full scan", entityType, e);
        }
        return Optional.empty();
    }

    private void lockTokenIndexForScan(IndexDescriptor index, LockManager.Client lockClient) {
        lockClient.initialize(LeaseService.NoLeaseClient.INSTANCE, SYSTEM_TRANSACTION_ID, INSTANCE, config);
        lockClient.acquireShared(
                LockTracer.NONE, index.schema().keyType(), index.schema().lockingKeys());
    }

    private record TokenIndexData(TokenIndexReader reader, IndexDescriptor descriptor) {}
}
