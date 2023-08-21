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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.StoreTokens.createReadOnlyTokenHolder;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;

import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

public class RecordStorageEngineTestUtils {
    public static RecordStorageEngine openSimpleStorageEngine(
            FileSystemAbstraction fs, PageCache pageCache, RecordDatabaseLayout layout, Config config) {
        TokenHolders tokenHolders = new TokenHolders(
                createReadOnlyTokenHolder(TokenHolder.TYPE_PROPERTY_KEY),
                createReadOnlyTokenHolder(TokenHolder.TYPE_LABEL),
                createReadOnlyTokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE));
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        LogTailMetadata emptyLogTailMetadata = new EmptyLogTailMetadata(config);
        return new RecordStorageEngine(
                layout,
                config,
                pageCache,
                fs,
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                tokenHolders,
                mock(SchemaState.class),
                new StandardConstraintRuleAccessor(),
                (c, indexingBehaviour) -> c,
                NO_LOCK_SERVICE,
                mock(DatabaseHealth.class),
                new DefaultIdGeneratorFactory(fs, immediate(), cacheTracer, DEFAULT_DATABASE_NAME),
                immediate(),
                EmptyMemoryTracker.INSTANCE,
                emptyLogTailMetadata,
                new MetadataCache(emptyLogTailMetadata),
                LockVerificationFactory.NONE,
                new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                cacheTracer);
    }

    public static void applyLogicalChanges(
            RecordStorageEngine storageEngine,
            ThrowingBiConsumer<ReadableTransactionState, TxStateVisitor, Exception> changes)
            throws Exception {
        ReadableTransactionState txState = mock(ReadableTransactionState.class);
        doAnswer(invocationOnMock -> {
                    TxStateVisitor visitor = invocationOnMock.getArgument(0);
                    changes.accept(txState, visitor);
                    return null;
                })
                .when(txState)
                .accept(any());
        when(txState.addedAndRemovedNodes()).thenReturn(LongDiffSets.EMPTY);
        when(txState.addedAndRemovedRelationships()).thenReturn(LongDiffSets.EMPTY);
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        TransactionIdStore txIdStore = neoStores.getMetaDataStore();
        KernelVersionProvider kernelVersionProvider = storageEngine.kernelVersionRepository;
        CursorContext cursorContext = NULL_CONTEXT;
        try (RecordStorageCommandCreationContext commandCreationContext =
                        storageEngine.newCommandCreationContext(false);
                StoreCursors storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
            commandCreationContext.initialize(
                    kernelVersionProvider,
                    cursorContext,
                    storeCursors,
                    CommandCreationContext.NO_STARTTIME_OF_OLDEST_TRANSACTION,
                    ResourceLocker.IGNORE,
                    () -> LockTracer.NONE);
            var commands = storageEngine.createCommands(
                    txState,
                    storageEngine.newReader(),
                    commandCreationContext,
                    LockTracer.NONE,
                    t -> t,
                    cursorContext,
                    storeCursors,
                    EmptyMemoryTracker.INSTANCE);
            storageEngine.apply(
                    new GroupOfCommands(
                            txIdStore.nextCommittingTransactionId(),
                            storeCursors,
                            commands.toArray(new StorageCommand[0])),
                    TransactionApplicationMode.EXTERNAL);
        }
    }
}
