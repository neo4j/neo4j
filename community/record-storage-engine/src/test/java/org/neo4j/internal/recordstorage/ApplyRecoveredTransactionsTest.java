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
package org.neo4j.internal.recordstorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class ApplyRecoveredTransactionsTest {
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    private NeoStores neoStores;
    private DefaultIdGeneratorFactory idGeneratorFactory;
    private CachedStoreCursors storeCursors;

    @BeforeEach
    void before() {
        var pageCacheTracer = PageCacheTracer.NULL;
        idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openAllNeoStores();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @AfterEach
    void after() {
        closeAllUnchecked(storeCursors, neoStores);
    }

    @Test
    void shouldSetCorrectHighIdWhenApplyingExternalTransactions() throws Exception {
        // WHEN recovering a transaction that creates some data
        long nodeId = neoStores.getNodeStore().getIdGenerator().nextId(NULL_CONTEXT);
        long relationshipId = neoStores.getRelationshipStore().getIdGenerator().nextId(NULL_CONTEXT);
        int type = 1;
        LogCommandSerialization serialization = LATEST_LOG_SERIALIZATION;
        applyExternalTransaction(
                1,
                new NodeCommand(serialization, new NodeRecord(nodeId), inUse(created(new NodeRecord(nodeId)))),
                new RelationshipCommand(
                        serialization,
                        null,
                        inUse(created(with(new RelationshipRecord(relationshipId), nodeId, nodeId, type)))));

        // and when, later on, recovering a transaction deleting some of those
        applyExternalTransaction(
                2,
                new NodeCommand(serialization, inUse(created(new NodeRecord(nodeId))), new NodeRecord(nodeId)),
                new RelationshipCommand(serialization, null, new RelationshipRecord(relationshipId)));

        // THEN that should be possible and the high ids should be correct, i.e. highest applied + 1
        assertEquals(nodeId + 1, neoStores.getNodeStore().getIdGenerator().getHighId());
        assertEquals(
                relationshipId + 1,
                neoStores.getRelationshipStore().getIdGenerator().getHighId());
    }

    private static RelationshipRecord with(RelationshipRecord relationship, long startNode, long endNode, int type) {
        relationship.setFirstNode(startNode);
        relationship.setSecondNode(endNode);
        relationship.setType(type);
        return relationship;
    }

    private void applyExternalTransaction(long transactionId, Command... commands) throws Exception {
        LockService lockService = mock(LockService.class);
        when(lockService.acquireNodeLock(anyLong(), any(LockType.class))).thenReturn(LockService.NO_LOCK);
        when(lockService.acquireRelationshipLock(anyLong(), any(LockType.class)))
                .thenReturn(LockService.NO_LOCK);
        IdGeneratorUpdatesWorkSync idGeneratorWorkSyncs = new IdGeneratorUpdatesWorkSync();
        Stream.of(RecordIdType.values()).forEach(idType -> idGeneratorWorkSyncs.add(idGeneratorFactory.get(idType)));

        LockGuardedNeoStoreTransactionApplierFactory applier = new LockGuardedNeoStoreTransactionApplierFactory(
                INTERNAL, neoStores, mock(CacheAccessBackDoor.class), lockService);
        StorageEngineTransaction tx = new GroupOfCommands(transactionId, storeCursors, commands);
        CommandHandlerContract.apply(
                applier,
                txApplier -> {
                    tx.accept(txApplier);
                    return false;
                },
                new GroupOfCommands(transactionId, storeCursors, commands));
    }

    private static <RECORD extends AbstractBaseRecord> RECORD inUse(RECORD record) {
        record.setInUse(true);
        return record;
    }

    private static <RECORD extends AbstractBaseRecord> RECORD created(RECORD record) {
        record.setCreated();
        return record;
    }
}
