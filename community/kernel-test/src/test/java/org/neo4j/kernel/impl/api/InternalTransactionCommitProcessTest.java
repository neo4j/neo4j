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
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.internal.helpers.Exceptions.contains;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.TestableTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

class InternalTransactionCommitProcessTest {
    private final TransactionWriteEvent transactionWriteEvent = TransactionWriteEvent.NULL;

    @Test
    void shouldFailWithProperMessageOnAppendException() throws Exception {
        // GIVEN
        TransactionAppender appender = mock(TransactionAppender.class);
        IOException rootCause = new IOException("Mock exception");
        doThrow(new IOException(rootCause))
                .when(appender)
                .append(any(TransactionToApply.class), any(LogAppendEvent.class));
        StorageEngine storageEngine = mock(StorageEngine.class);
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, false, commandCommitListeners);

        // WHEN
        var mockedTransaction = mockedTransaction(mock(TransactionIdStore.class));
        TransactionFailureException exception = assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(mockedTransaction, transactionWriteEvent, INTERNAL));

        assertThat(exception.getMessage()).contains("Could not append transaction: ");
        assertTrue(contains(exception, rootCause.getMessage(), rootCause.getClass()));
        verify(commandCommitListeners).registerFailure(mockedTransaction.commandBatch(), exception);
    }

    @Test
    void shouldCloseTransactionRegardlessOfWhetherOrNotItAppliedCorrectly() throws Exception {
        // GIVEN
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        TransactionAppender appender = new TestableTransactionAppender();
        long txId = 11;
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
        IOException rootCause = new IOException("Mock exception");
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new IOException(rootCause))
                .when(storageEngine)
                .apply(any(TransactionToApply.class), any(TransactionApplicationMode.class));
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, false, commandCommitListeners);
        TransactionToApply transaction = mockedTransaction(transactionIdStore);

        // WHEN
        TransactionFailureException exception = assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(transaction, transactionWriteEvent, INTERNAL));
        assertThat(exception.getMessage()).contains("Could not apply the transaction:");
        assertTrue(contains(exception, rootCause.getMessage(), rootCause.getClass()));
        verify(commandCommitListeners).registerFailure(transaction.commandBatch(), exception);
        verify(commandCommitListeners, never()).registerSuccess(any(), anyLong());

        // THEN
        // we can't verify transactionCommitted since that's part of the TransactionAppender, which we have mocked
        verify(transactionIdStore)
                .transactionClosed(
                        eq(txId),
                        anyLong(),
                        any(KernelVersion.class),
                        anyLong(),
                        anyLong(),
                        anyInt(),
                        anyLong(),
                        anyLong());
    }

    @Test
    void shouldSuccessfullyCommitTransactionWithNoCommands() throws Exception {
        // GIVEN
        long txId = 11;
        long appendIndex = txId + 7;

        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        TransactionAppender appender = new TestableTransactionAppender();
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);

        StorageEngine storageEngine = mock(StorageEngine.class);

        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, false, commandCommitListeners);
        CompleteTransaction noCommandTx = new CompleteTransaction(
                Collections.emptyList(),
                UNKNOWN_CONSENSUS_INDEX,
                -1,
                -1,
                -1,
                -1,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);

        // WHEN

        var transactionToApply = new TransactionToApply(
                noCommandTx,
                NULL_CONTEXT,
                StoreCursors.NULL,
                new FakeCommitment(txId, appendIndex, transactionIdStore, true),
                new IdStoreTransactionIdGenerator(transactionIdStore));
        commitProcess.commit(transactionToApply, transactionWriteEvent, INTERNAL);

        verify(transactionIdStore)
                .transactionCommitted(
                        txId,
                        appendIndex,
                        DEFAULT_BOOTSTRAP_VERSION,
                        FakeCommitment.CHECKSUM,
                        FakeCommitment.TIMESTAMP,
                        FakeCommitment.CONSENSUS_INDEX);
        verify(commandCommitListeners, never()).registerFailure(any(), any());
        verify(commandCommitListeners).registerSuccess(transactionToApply.commandBatch(), txId);
    }

    @Test
    void shouldFailWithOutOfDiskSpaceOnPreAllocationException() throws Exception {
        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new OutOfDiskSpaceException("test out of disk"))
                .when(storageEngine)
                .preAllocateStoreFilesForCommands(any(), any());
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, true, commandCommitListeners);

        var transaction = mockedTransaction(mock(TransactionIdStore.class));
        TransactionFailureException exception = assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(transaction, transactionWriteEvent, INTERNAL));
        assertThat(exception.getMessage()).contains("Could not preallocate disk space ");
        // FIXME ODP this is not the status we should end up with in the end
        assertThat(exception.status()).isEqualTo(Status.General.UnknownError);
        assertTrue(contains(exception, "test out of disk", OutOfDiskSpaceException.class));
        verify(commandCommitListeners).registerFailure(transaction.commandBatch(), exception);
    }

    @Test
    void shouldNotReportOutOfDiskSpaceOnGeneralIOException() throws Exception {
        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new IOException("IO exception other than out of disk"))
                .when(storageEngine)
                .preAllocateStoreFilesForCommands(any(), any());
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, true, commandCommitListeners);

        var transaction = mockedTransaction(mock(TransactionIdStore.class));
        TransactionFailureException exception = assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(transaction, transactionWriteEvent, INTERNAL));
        assertThat(exception.getMessage()).contains("Could not preallocate disk space ");
        assertThat(exception.status()).isEqualTo(Status.Transaction.TransactionCommitFailed);
        assertTrue(contains(exception, "IO exception other than out of disk", IOException.class));
        verify(commandCommitListeners).registerFailure(transaction.commandBatch(), exception);
    }

    @Test
    void shouldNotTryToPreallocateWhenDisabled() throws IOException, TransactionFailureException {
        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess =
                new InternalTransactionCommitProcess(appender, storageEngine, false, commandCommitListeners);
        commitProcess.commit(mockedTransaction(mock(TransactionIdStore.class)), transactionWriteEvent, INTERNAL);

        verify(storageEngine, never()).preAllocateStoreFilesForCommands(any(), any());
    }

    private TransactionToApply mockedTransaction(TransactionIdStore transactionIdStore) {
        CommandBatch batch = mock(CommandBatch.class);
        when(batch.consensusIndex()).thenReturn(UNKNOWN_CONSENSUS_INDEX);
        when(batch.kernelVersion()).thenReturn(LatestVersions.LATEST_KERNEL_VERSION);
        var commitmentFactory = new TransactionCommitmentFactory(transactionIdStore);
        var transactionCommitment = commitmentFactory.newCommitment();
        return new TransactionToApply(
                batch,
                NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }
}
