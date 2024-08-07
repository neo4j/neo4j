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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.EmptyIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.token.TokenHolders;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class PreAllocationOfStoreFilesTest {
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Inject
    private PageCache pageCache;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private RecordStorageEngine recordStorageEngine;
    private PagedFile fakeNodeStore;
    private PagedFile fakeRelStore;

    @BeforeEach
    void before() throws IOException {
        PageCache spy = Mockito.spy(pageCache);
        RecordDatabaseLayout recordDatabaseLayout = RecordDatabaseLayout.cast(databaseLayout);
        fakeNodeStore = mock(PagedFile.class);
        fakeRelStore = mock(PagedFile.class);
        doReturn(fakeNodeStore).when(spy).map(eq(recordDatabaseLayout.nodeStore()), anyInt(), anyString(), any());
        doReturn(fakeRelStore)
                .when(spy)
                .map(eq(recordDatabaseLayout.relationshipStore()), anyInt(), anyString(), any());

        recordStorageEngine = new RecordStorageEngine(
                recordDatabaseLayout,
                Config.defaults(),
                spy,
                fs,
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                mock(TokenHolders.class),
                mock(SchemaState.class),
                mock(ConstraintRuleAccessor.class),
                mock(IndexConfigCompleter.class),
                LockService.NO_LOCK_SERVICE,
                mock(DatabaseHealth.class),
                EmptyIdGeneratorFactory.EMPTY_ID_GENERATOR_FACTORY,
                RecoveryCleanupWorkCollector.ignore(),
                EmptyMemoryTracker.INSTANCE,
                new EmptyLogTailMetadata(Config.defaults()),
                mock(KernelVersionRepository.class),
                LockVerificationFactory.NONE,
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                VersionStorage.EMPTY_STORAGE);
    }

    @AfterEach
    void after() {
        recordStorageEngine.shutdown();
    }

    @Test
    void shouldReserveSpaceOnPreallocation() throws IOException {
        CompleteCommandBatch storageCommands = new CompleteCommandBatch(
                List.of(
                        new NodeCommand(
                                LATEST_LOG_SERIALIZATION,
                                new NodeRecord(1000),
                                new NodeRecord(1000).initialize(true, -1, false, -1, 1)),
                        new NodeCommand(
                                LATEST_LOG_SERIALIZATION,
                                new NodeRecord(1001),
                                new NodeRecord(1001).initialize(true, -1, false, -1, 1)),
                        new NodeCommand(
                                LATEST_LOG_SERIALIZATION,
                                new NodeRecord(500),
                                new NodeRecord(500).initialize(true, -1, false, -1, 1)),
                        new RelationshipCommand(
                                LATEST_LOG_SERIALIZATION,
                                new RelationshipRecord(2000),
                                new RelationshipRecord(2000)
                                        .initialize(true, -1, -1, -1, 1, -1, -1, -1, -1, true, true))),
                UNKNOWN_CONSENSUS_INDEX,
                1611333951,
                2,
                1611777951,
                5,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        CommittedTransactionRepresentation transaction = new CommittedTransactionRepresentation(
                mock(LogEntryStart.class), storageCommands, mock(LogEntryCommit.class));
        CompleteTransaction completeTransaction = new CompleteTransaction(transaction, NULL_CONTEXT, StoreCursors.NULL);

        recordStorageEngine.preAllocateStoreFilesForCommands(completeTransaction, TransactionApplicationMode.INTERNAL);

        // The stores should have been preallocated once for each affected store with the highest pageId needed
        Mockito.verify(fakeNodeStore).pageReservedBytes();
        Mockito.verify(fakeNodeStore).preAllocate(eq(1L));
        verifyNoMoreInteractions(fakeNodeStore);
        Mockito.verify(fakeRelStore).pageReservedBytes();
        Mockito.verify(fakeRelStore).preAllocate(eq(8L));
        verifyNoMoreInteractions(fakeRelStore);
    }
}
