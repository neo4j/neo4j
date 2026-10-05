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
package org.neo4j.kernel.impl.api.commit;

import static java.util.Collections.emptyList;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;

import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.TransactionRollbackException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.StorageCommands;
import org.neo4j.kernel.impl.api.TransactionClockContext;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.api.chunk.TransactionRollbackProcess;
import org.neo4j.kernel.impl.api.transaction.serial.SerialExecutionGuard;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.ValidationLockDumper;
import org.neo4j.wal.TransactionCommitmentFactory;

public final class ChunkCommitter implements TransactionCommitter {
    private final KernelTransactionImplementation ktx;
    private int chunkNumber = BASE_CHUNK_ID;
    private long previousBatchAppendIndex = UNKNOWN_APPEND_INDEX;
    private KernelVersion kernelVersion;
    private Lock raftUpgradeBarrierEntered;
    private ChunkedTransaction transactionPayload;
    private final TransactionCommitmentFactory commitmentFactory;
    private final KernelVersionProvider kernelVersionProvider;
    private final StoreCursors transactionalCursors;
    private final TransactionIdGenerator transactionIdGenerator;
    private final TransactionCommitProcess commitProcess;
    private final DatabaseHealth databaseHealth;
    private final TransactionClockContext clocks;
    private final TransactionRollbackProcess chunkedRollbackProcess;
    private final TransactionValidator transactionValidator;
    private final ValidationLockDumper validationLockDumper;
    private final SerialExecutionGuard serialExecutionGuard;
    private final Log log;
    private final HostedOnMode mode;
    private long lastTransactionIdWhenStarted;
    private long startTimeMillis;
    private LeaseClient leaseClient;

    public ChunkCommitter(
            KernelTransactionImplementation ktx,
            TransactionCommitmentFactory commitmentFactory,
            KernelVersionProvider kernelVersionProvider,
            StoreCursors transactionalCursors,
            TransactionIdGenerator transactionIdGenerator,
            TransactionCommitProcess commitProcess,
            DatabaseHealth databaseHealth,
            TransactionClockContext clocks,
            TransactionRollbackProcess chunkedRollbackProcess,
            TransactionValidator transactionValidator,
            ValidationLockDumper validationLockDumper,
            SerialExecutionGuard serialExecutionGuard,
            LogProvider logProvider,
            HostedOnMode mode) {
        this.ktx = ktx;
        this.commitmentFactory = commitmentFactory;
        this.kernelVersionProvider = kernelVersionProvider;
        this.transactionalCursors = transactionalCursors;
        this.transactionIdGenerator = transactionIdGenerator;
        this.commitProcess = commitProcess;
        this.databaseHealth = databaseHealth;
        this.clocks = clocks;
        this.chunkedRollbackProcess = chunkedRollbackProcess;
        this.transactionValidator = transactionValidator;
        this.validationLockDumper = validationLockDumper;
        this.serialExecutionGuard = serialExecutionGuard;
        this.log = logProvider.getLog(ChunkCommitter.class);
        this.mode = mode;
    }

    @Override
    public long commit(
            TransactionWriteEvent transactionWriteEvent,
            LeaseClient leaseClient,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            KernelTransaction.Monitor monitor,
            LockTracer lockTracer,
            long commitTime,
            long startTimeMillis,
            long lastTransactionIdWhenStarted,
            boolean commit,
            TransactionApplicationMode mode)
            throws KernelException {
        LockManager.Client lockClient = ktx.lockClient();
        try {
            if (raftUpgradeBarrierEntered == null) {
                // Hold the upgrade barrier from the moment we capture the version in commands until this transaction
                // has finished appending all its chunks (released in reset()), so a concurrent version upgrade cannot
                // be ordered before a later chunk of this (now old-version) transaction.
                raftUpgradeBarrierEntered = ktx.enterRaftUpgradeBarrier();
            }
            if (commit) {
                ktx.txState().markAsFinalChunk();
            }
            StorageCommands storageCommands = ktx.extractCommands(memoryTracker);
            List<StorageCommand> extractedCommands = storageCommands.commands();
            assert storageCommands.leases().size() == 0 : "LeaseMap not implemented for chunked transactions";
            if (!extractedCommands.isEmpty() || (commit && transactionPayload != null)) {
                serialExecutionGuard.check();
                if (kernelVersion == null) {
                    this.kernelVersion = kernelVersionProvider.kernelVersion();
                    this.lastTransactionIdWhenStarted = lastTransactionIdWhenStarted;
                    this.startTimeMillis = lastTransactionIdWhenStarted;
                    this.leaseClient = leaseClient;
                }
                if (commit) {
                    validateCurrentKernelVersion();
                }
                try {
                    transactionValidator.validate(
                            extractedCommands, cursorContext, lockClient, lockTracer, validationLockDumper);
                    var chunkMetadata = new ChunkMetadata(
                            chunkNumber == BASE_CHUNK_ID,
                            commit,
                            false,
                            previousBatchAppendIndex,
                            chunkNumber,
                            new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                            new MutableLong(UNKNOWN_APPEND_INDEX),
                            startTimeMillis,
                            lastTransactionIdWhenStarted,
                            commitTime,
                            leaseClient.leaseId(),
                            kernelVersion,
                            ktx.securityContext().subject().userSubject());
                    var transaction = getTransaction(cursorContext);

                    ChunkedCommandBatch chunk = new ChunkedCommandBatch(extractedCommands, chunkMetadata);
                    transaction.init(chunk);
                    commitProcess.commit(transaction, transactionWriteEvent, mode, memoryTracker);

                    // transaction chunk commit completed
                    transactionPayload = transaction;

                    validationLockDumper.dumpLocks(
                            lockClient, chunkNumber, transactionPayload.transactionId(), memoryTracker);
                    transactionWriteEvent.chunkAppended(
                            chunkNumber,
                            ktx.getTransactionSequenceNumber(),
                            transactionPayload.transactionId(),
                            transactionPayload.lastBatchAppendIndex());
                } catch (TransactionConflictException tce) {
                    dumpLocks(lockClient, memoryTracker);
                    throw tce;
                } catch (Exception e) {
                    dumpLocks(lockClient, memoryTracker);
                    log.debug("Transaction chunk commit failure.", e);
                    throw e;
                }
                previousBatchAppendIndex = transactionPayload.lastBatchAppendIndex();
                chunkNumber++;
            }
            return transactionPayload != null ? transactionPayload.transactionId() : KernelTransaction.READ_ONLY_ID;
        } finally {
            lockClient.reset();
        }
    }

    private void dumpLocks(LockManager.Client lockClient, MemoryTracker memoryTracker) {
        long txId = transactionPayload != null ? transactionPayload.transactionId() : UNKNOWN_TX_ID;
        validationLockDumper.dumpLocks(lockClient, chunkNumber, txId, memoryTracker);
    }

    @Override
    public void rollback(TransactionRollbackEvent rollbackEvent) {
        if (transactionPayload != null) {
            try {
                validateCurrentKernelVersion();
                prepareRollBackEntry();
                if (isSingleInstance()) {
                    chunkedRollbackProcess.rollbackChunks(transactionPayload, rollbackEvent, ktx.memoryTracker());
                }
                writeRollbackEntry(rollbackEvent);
            } catch (Exception e) {
                if (isSingleInstance()) {
                    databaseHealth.panic(e);
                }
                Exceptions.throwIfInstanceOf(e, TransactionRollbackException.class);
                throw new TransactionRollbackException("Transaction rollback failed", e);
            }
        }
    }

    private boolean isSingleInstance() {
        return HostedOnMode.SINGLE == mode;
    }

    private ChunkedTransaction getTransaction(CursorContext cursorContext) {
        return transactionPayload != null
                ? transactionPayload
                : new ChunkedTransaction(
                        cursorContext,
                        ktx.getTransactionSequenceNumber(),
                        transactionalCursors,
                        commitmentFactory.newCommitment(),
                        transactionIdGenerator);
    }

    private void writeRollbackEntry(TransactionRollbackEvent transactionRollbackEvent)
            throws TransactionFailureException {
        try (var writeEvent = transactionRollbackEvent.beginRollbackWriteEvent()) {
            commitProcess.commit(transactionPayload, writeEvent, INTERNAL, ktx.memoryTracker());
        }
    }

    private void prepareRollBackEntry() {
        var chunkMetadata = new ChunkMetadata(
                false,
                true,
                true,
                previousBatchAppendIndex,
                chunkNumber,
                new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                new MutableLong(UNKNOWN_APPEND_INDEX),
                startTimeMillis,
                lastTransactionIdWhenStarted,
                clocks.systemClock().millis(),
                leaseClient.leaseId(),
                kernelVersion,
                ktx.securityContext().subject().userSubject());
        ChunkedCommandBatch chunk = new ChunkedCommandBatch(emptyList(), chunkMetadata);
        transactionPayload.init(chunk);
    }

    private void validateCurrentKernelVersion() {
        if (kernelVersion != kernelVersionProvider.kernelVersion()) {
            throw new UnsupportedOperationException(
                    "Kernel version changed while running chunked transaction. Please check kernel version upgrade path.");
        }
    }

    @Override
    public void reset() {
        if (raftUpgradeBarrierEntered != null) {
            raftUpgradeBarrierEntered.close();
            raftUpgradeBarrierEntered = null;
        }
        chunkNumber = BASE_CHUNK_ID;
        kernelVersion = null;
        transactionPayload = null;
        lastTransactionIdWhenStarted = 0;
        startTimeMillis = 0;
        leaseClient = null;
    }
}
