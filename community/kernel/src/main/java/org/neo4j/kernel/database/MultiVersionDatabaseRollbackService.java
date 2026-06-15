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
package org.neo4j.kernel.database;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.util.Collection;
import java.util.Collections;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.common.Subject;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionIdSequence;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.time.SystemNanoClock;

public class MultiVersionDatabaseRollbackService extends LifecycleAdapter {

    private final KernelTransactions kernelTransactions;
    private final InternalLog internalLog;
    private final DatabaseTracers tracers;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final LeaseService leaseService;
    private final ChunkedTransactionTracker chunkedTransactionTracker;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;
    private final DatabaseHealth databaseHealth;
    private final TransactionCommitProcess transactionCommitProcess;
    private final TransactionIdSequence transactionIdSequence;
    private final SystemNanoClock clock;
    private boolean shutdown;

    public MultiVersionDatabaseRollbackService(
            KernelTransactions kernelTransactions,
            InternalLog internalLog,
            DatabaseTracers tracers,
            DatabaseAvailabilityGuard databaseAvailabilityGuard,
            LeaseService leaseService,
            ChunkedTransactionTracker chunkedTransactionTracker,
            DatabaseReadOnlyChecker readOnlyDatabaseChecker,
            DatabaseHealth databaseHealth,
            TransactionCommitProcess transactionCommitProcess,
            TransactionIdSequence transactionIdSequence,
            SystemNanoClock clock) {
        this.kernelTransactions = kernelTransactions;
        this.internalLog = internalLog;
        this.tracers = tracers;
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.leaseService = leaseService;
        this.chunkedTransactionTracker = chunkedTransactionTracker;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
        this.databaseHealth = databaseHealth;
        this.transactionCommitProcess = transactionCommitProcess;
        this.transactionIdSequence = transactionIdSequence;
        this.clock = clock;
    }

    public synchronized void postLeaseSwitchTransactionCleanup(int leaseId) {
        try {
            if (shutdown) {
                return;
            }
            LeaseClient leaseClient = leaseService.newClient();
            leaseClient.ensureValid();
            if (leaseService != LeaseService.NO_LEASES && leaseClient.leaseId() != leaseId) {
                internalLog.debug("Lease expired while doing database rollback.");
                return;
            }
            kernelTransactions.terminateOldLeaseTransactions(leaseId);

            if (readOnlyDatabaseChecker.isReadOnly()) {
                internalLog.info("Post lease switch transaction can't be executed on the read only database.");
                return;
            }

            DatabaseTracer databaseTracer = tracers.getDatabaseTracer();
            try (var databaseAsyncRollbackEvent = databaseTracer.beginAsyncDatabaseRollback()) {

                Collection<ChunkedTransactionTracker.TransactionInfo> transactionInfos =
                        chunkedTransactionTracker.transactionsToRollback();
                if (transactionInfos.isEmpty()) {
                    internalLog.debug("Post lease switch transaction cleanup had no transactions to cleanup.");
                    databaseAsyncRollbackEvent.databaseRollbackCompleted(true, 0, 0);
                    return;
                }

                internalLog.info("Post lease switch transaction cleanup has " + transactionInfos.size()
                        + " candidates to rollback.");
                int rolledBackTransactions = 0;
                int chunkedOngoingTransactions = 0;
                boolean completedSuccessfully = false;

                try {
                    for (ChunkedTransactionTracker.TransactionInfo transactionInfo : transactionInfos) {
                        if (databaseAvailabilityGuard.isShutdown()) {
                            return;
                        }
                        if (transactionInfo.leaseId() == leaseId) {
                            chunkedOngoingTransactions++;
                            continue;
                        }

                        try (TransactionRollbackEvent transactionRollbackEvent =
                                databaseAsyncRollbackEvent.beginAsyncTransactionRollback()) {
                            long time = clock.millis();
                            ChunkMetadata chunkMetadata = createChunkMetadata(transactionInfo, time, leaseClient);
                            var chunkedTransaction = new ChunkedTransaction(
                                    transactionInfo.transactionId(),
                                    transactionIdSequence.next(),
                                    transactionInfo.lastBatchAppendIndex(),
                                    CursorContext.NULL_CONTEXT,
                                    StoreCursors.NULL,
                                    new ChunkedCommandBatch(Collections.emptyList(), chunkMetadata));
                            try (TransactionWriteEvent transactionWriteEvent =
                                    transactionRollbackEvent.beginRollbackWriteEvent()) {
                                transactionCommitProcess.commit(
                                        chunkedTransaction, transactionWriteEvent, TransactionApplicationMode.INTERNAL);
                            }
                            chunkedTransactionTracker.cleanupChunkedTransaction(transactionInfo.transactionId());
                        }
                        rolledBackTransactions++;
                    }
                    completedSuccessfully = true;
                } finally {
                    internalLog.info("Post lease switch transaction cleanup completed"
                            + (completedSuccessfully ? " successfully." : " partially.") + " Completed rollback of "
                            + rolledBackTransactions + " and skipped " + chunkedOngoingTransactions + " transactions.");
                }
                databaseAsyncRollbackEvent.databaseRollbackCompleted(
                        completedSuccessfully, rolledBackTransactions, chunkedOngoingTransactions);
            }
        } catch (LeaseException ignored) {
            internalLog.debug("Lease expired while doing database rollback.");
        } catch (Exception e) {
            internalLog.error("Unexpected error while doing database rollback.", e);
            databaseHealth.panic(e);
        }
    }

    @Override
    public synchronized void shutdown() throws Exception {
        shutdown = true;
    }

    private static ChunkMetadata createChunkMetadata(
            ChunkedTransactionTracker.TransactionInfo transactionInfo, long time, LeaseClient leaseClient) {
        return new ChunkMetadata(
                false,
                true,
                true,
                transactionInfo.lastBatchAppendIndex(),
                transactionInfo.chunkId() + 1,
                new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                new MutableLong(UNKNOWN_APPEND_INDEX),
                time,
                -1,
                time,
                leaseClient.leaseId(),
                transactionInfo.kernelVersion(),
                Subject.AUTH_DISABLED);
    }
}
