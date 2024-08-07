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
package org.neo4j.tracers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent.NULL;
import static org.neo4j.lock.ResourceLocker.IGNORE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockTracer;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class CommitProcessTracingIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private KernelVersionProvider kernelVersionProvider;

    @Inject
    private TransactionCommitProcess commitProcess;

    @Inject
    private StorageEngine storageEngine;

    @Inject
    private TransactionCommitmentFactory transactionCommitmentFactory;

    @Inject
    private TransactionIdGenerator transactionIdGenerator;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        // Disable the additional lock verification since this tests really only uses the raw storage engine
        builder.setConfig(GraphDatabaseInternalSettings.additional_lock_verification, false);
        builder.setConfig(db_format, RecordFormatSelector.defaultFormat().name());
    }

    @Test
    void tracePageCacheAccessOnCommandCreation() throws KernelException {
        long sourceId;
        try (Transaction transaction = database.beginTx()) {
            sourceId = transaction.createNode(Label.label("a")).getId();
            transaction.commit();
        }

        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnCommandCreation");
                var reader = storageEngine.newReader()) {
            assertZeroCursor(cursorContext);
            try (CommandCreationContext context = storageEngine.newCommandCreationContext(false);
                    var storeCursors = storageEngine.createStorageCursors(cursorContext)) {
                context.initialize(
                        kernelVersionProvider,
                        cursorContext,
                        storeCursors,
                        CommandCreationContext.NO_STARTTIME_OF_OLDEST_TRANSACTION,
                        IGNORE,
                        () -> LockTracer.NONE);
                var txState = new TxState();
                txState.nodeDoAddLabel(1, sourceId);
                storageEngine.createCommands(
                        txState,
                        reader,
                        context,
                        LockTracer.NONE,
                        NO_DECORATION,
                        cursorContext,
                        storeCursors,
                        INSTANCE);
            }
            assertCursor(cursorContext, 1);
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionApply() throws TransactionFailureException {
        var transaction = new CompleteCommandBatch(
                List.of(new Command.NodeCountsCommand(
                        RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION), 1, 2)),
                UNKNOWN_CONSENSUS_INDEX,
                0,
                0,
                0,
                0,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnTransactionApply")) {
            assertZeroCursor(cursorContext);

            commitProcess.commit(
                    new CompleteTransaction(
                            transaction,
                            cursorContext,
                            StoreCursors.NULL,
                            transactionCommitmentFactory.newCommitment(),
                            transactionIdGenerator),
                    NULL,
                    EXTERNAL);

            assertCursor(cursorContext, 1);
        }
    }

    private static void assertCursor(CursorContext cursorContext, int expected) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(expected);
        assertThat(cursorTracer.unpins()).isEqualTo(expected);
        assertThat(cursorTracer.hits()).isEqualTo(expected);
    }

    private static void assertZeroCursor(CursorContext cursorContext) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
        assertThat(cursorTracer.hits()).isZero();
    }
}
