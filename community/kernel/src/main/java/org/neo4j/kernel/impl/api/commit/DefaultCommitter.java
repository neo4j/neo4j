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

import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.util.List;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public final class DefaultCommitter implements TransactionCommitter {
    private final KernelTransactionImplementation ktx;
    private final TransactionCommitmentFactory commitmentFactory;
    private final KernelVersionProvider kernelVersionProvider;
    private final TransactionCommitProcess commitProcess;
    private final StoreCursors transactionalCursors;
    private final TransactionIdGenerator transactionIdGenerator;

    public DefaultCommitter(
            KernelTransactionImplementation ktx,
            TransactionCommitmentFactory commitmentFactory,
            KernelVersionProvider kernelVersionProvider,
            StoreCursors transactionalCursors,
            TransactionIdGenerator transactionIdGenerator,
            TransactionCommitProcess commitProcess) {
        this.ktx = ktx;
        this.commitmentFactory = commitmentFactory;
        this.kernelVersionProvider = kernelVersionProvider;
        this.commitProcess = commitProcess;
        this.transactionalCursors = transactionalCursors;
        this.transactionIdGenerator = transactionIdGenerator;
    }

    @Override
    public long commit(
            TransactionWriteEvent transactionWriteEvent,
            LeaseClient leaseClient,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            KernelTransaction.KernelTransactionMonitor kernelTransactionMonitor,
            LockTracer lockTracer,
            long commitTime,
            long startTimeMillis,
            long lastTransactionIdWhenStarted,
            boolean commit)
            throws KernelException {
        // Gather-up commands from the various sources
        List<StorageCommand> extractedCommands = ktx.extractCommands(memoryTracker);

        /* Here's the deal: we track a quick-to-access hasChanges in transaction state which is true
         * if there are any changes imposed by this transaction. Some changes made inside a transaction undo
         * previously made changes in that same transaction, and so at some point a transaction may have
         * changes and at another point, after more changes seemingly,
         * the transaction may not have any changes.
         * However, to track that "undoing" of the changes is a bit tedious, intrusive and hard to maintain
         * and get right.... So to really make sure the transaction has changes we re-check by looking if we
         * have produced any commands to add to the logical log.
         */
        if (!extractedCommands.isEmpty()) {
            // Finish up the whole transaction representation

            CompleteTransaction transactionRepresentation = new CompleteTransaction(
                    extractedCommands,
                    UNKNOWN_CONSENSUS_INDEX,
                    startTimeMillis,
                    lastTransactionIdWhenStarted,
                    commitTime,
                    leaseClient.leaseId(),
                    kernelVersionProvider.kernelVersion(),
                    ktx.securityContext().subject().userSubject());

            // Commit the transaction
            TransactionToApply batch = new TransactionToApply(
                    transactionRepresentation,
                    cursorContext,
                    transactionalCursors,
                    commitmentFactory.newCommitment(),
                    transactionIdGenerator);

            kernelTransactionMonitor.beforeApply();
            return commitProcess.commit(batch, transactionWriteEvent, INTERNAL);
        }
        return KernelTransaction.READ_ONLY_ID;
    }

    @Override
    public void rollback(TransactionRollbackEvent rollbackEvent) {
        // default implementation does not do any transaction related rollbacks
    }
}
