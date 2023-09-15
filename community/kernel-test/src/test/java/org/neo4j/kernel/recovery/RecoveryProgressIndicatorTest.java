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
package org.neo4j.kernel.recovery;

import static java.util.Collections.emptyList;
import static org.apache.commons.io.IOUtils.EMPTY_BYTE_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.kernel.recovery.RecoveryStartupChecker.EMPTY_CHECKER;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.common.ProgressReporter;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class RecoveryProgressIndicatorTest {

    @Test
    void reportProgressOnRecovery() throws Throwable {
        RecoveryService recoveryService = mock(RecoveryService.class, Answers.RETURNS_MOCKS);
        CorruptedLogsTruncator logsTruncator = mock(CorruptedLogsTruncator.class);
        RecoveryMonitor recoveryMonitor = mock(RecoveryMonitor.class);
        CommandBatchCursor reverseCommandBatchCursor = mock(CommandBatchCursor.class);
        CommandBatchCursor commandBatchCursor = mock(CommandBatchCursor.class);

        int transactionsToRecover = 5;
        int expectedMax = transactionsToRecover * 2;
        int lastCommittedTransactionId = 14;
        CommittedTransactionRepresentation transactionRepresentation = new CommittedTransactionRepresentation(
                newStartEntry(LATEST_KERNEL_VERSION, 1, 2, 3, EMPTY_BYTE_ARRAY, LogPosition.UNSPECIFIED),
                emptyList(),
                newCommitEntry(LATEST_KERNEL_VERSION, lastCommittedTransactionId, 1L, BASE_TX_CHECKSUM));
        LogPosition transactionLogPosition = new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize());
        LogPosition checkpointLogPosition = new LogPosition(0, LATEST_LOG_FORMAT.getHeaderSize());
        int firstTxIdAfterLastCheckPoint = 10;
        RecoveryStartInformation startInformation = new RecoveryStartInformation(
                transactionLogPosition, checkpointLogPosition, firstTxIdAfterLastCheckPoint);

        when(reverseCommandBatchCursor.next()).thenAnswer(new NextTransactionAnswer(transactionsToRecover));
        when(commandBatchCursor.next()).thenAnswer(new NextTransactionAnswer(transactionsToRecover));
        when(reverseCommandBatchCursor.get()).thenReturn(transactionRepresentation);
        when(commandBatchCursor.get()).thenReturn(transactionRepresentation);

        when(recoveryService.getRecoveryStartInformation()).thenReturn(startInformation);
        when(recoveryService.getCommandBatchesInReverseOrder(transactionLogPosition))
                .thenReturn(reverseCommandBatchCursor);
        when(recoveryService.getCommandBatches(transactionLogPosition)).thenReturn(commandBatchCursor);

        AssertableProgressReporter progressReporter = new AssertableProgressReporter(expectedMax);
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        TransactionLogsRecovery recovery = new TransactionLogsRecovery(
                recoveryService,
                logsTruncator,
                new LifecycleAdapter(),
                recoveryMonitor,
                progressReporter,
                true,
                EMPTY_CHECKER,
                RecoveryPredicate.ALL,
                contextFactory,
                RecoveryMode.FULL);
        recovery.init();

        progressReporter.verify();
    }

    private static class AssertableProgressReporter implements ProgressReporter {
        private final int expectedMax;
        private int recoveredTransactions;
        private long max;
        private boolean completed;

        AssertableProgressReporter(int expectedMax) {
            this.expectedMax = expectedMax;
        }

        @Override
        public void start(long max) {
            this.max = max;
        }

        @Override
        public void progress(long add) {
            recoveredTransactions += add;
        }

        @Override
        public void completed() {
            completed = true;
        }

        public void verify() {
            assertTrue(completed, "Progress reporting was not completed.");
            assertEquals(expectedMax, max, "Number of max recovered transactions is different.");
            assertEquals(expectedMax, recoveredTransactions, "Number of recovered transactions is different.");
        }
    }

    private static class NextTransactionAnswer implements Answer<Boolean> {
        private final int expectedTransactionsToRecover;
        private int invocations;

        NextTransactionAnswer(int expectedTransactionsToRecover) {
            this.expectedTransactionsToRecover = expectedTransactionsToRecover;
        }

        @Override
        public Boolean answer(InvocationOnMock invocationOnMock) {
            invocations++;
            return invocations <= expectedTransactionsToRecover;
        }
    }
}
