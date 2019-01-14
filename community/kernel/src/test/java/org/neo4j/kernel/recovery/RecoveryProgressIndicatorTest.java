/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import org.junit.Test;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoveryProgressIndicatorTest
{

    @Test
    public void reportProgressOnRecovery() throws Throwable
    {
        RecoveryService recoveryService = mock( RecoveryService.class, Answers.RETURNS_MOCKS );
        StartupStatisticsProvider statisticsProvider = mock( StartupStatisticsProvider.class );
        CorruptedLogsTruncator logsTruncator = mock( CorruptedLogsTruncator.class );
        RecoveryMonitor recoveryMonitor = mock( RecoveryMonitor.class );
        TransactionCursor reverseTransactionCursor = mock( TransactionCursor.class );
        TransactionCursor transactionCursor = mock( TransactionCursor.class );
        CommittedTransactionRepresentation transactionRepresentation = mock( CommittedTransactionRepresentation.class );

        int transactionsToRecover = 5;
        int expectedMax = transactionsToRecover * 2;
        int lastCommittedTransactionId = 14;
        LogPosition recoveryStartPosition = LogPosition.start( 0 );
        int firstTxIdAfterLastCheckPoint = 10;
        RecoveryStartInformation startInformation = new RecoveryStartInformation( recoveryStartPosition, firstTxIdAfterLastCheckPoint );

        when( reverseTransactionCursor.next() ).thenAnswer( new NextTransactionAnswer( transactionsToRecover ) );
        when( transactionCursor.next() ).thenAnswer( new NextTransactionAnswer( transactionsToRecover ) );
        when( reverseTransactionCursor.get() ).thenReturn( transactionRepresentation );
        when( transactionCursor.get() ).thenReturn( transactionRepresentation );
        when( transactionRepresentation.getCommitEntry() ).thenReturn( new LogEntryCommit( lastCommittedTransactionId, 1L ) );

        when( recoveryService.getRecoveryStartInformation() ).thenReturn( startInformation );
        when( recoveryService.getTransactionsInReverseOrder( recoveryStartPosition ) ).thenReturn( reverseTransactionCursor );
        when( recoveryService.getTransactions( recoveryStartPosition ) ).thenReturn( transactionCursor );

        AssertableProgressReporter progressReporter = new AssertableProgressReporter( expectedMax );
        Recovery recovery = new Recovery( recoveryService, statisticsProvider, logsTruncator, recoveryMonitor,
                progressReporter, true );
        recovery.init();

        progressReporter.verify();
    }

    private static class AssertableProgressReporter implements ProgressReporter
    {
        private final int expectedMax;
        private int recoveredTransactions;
        private long max;
        private boolean completed;

        AssertableProgressReporter( int expectedMax )
        {
            this.expectedMax = expectedMax;
        }

        @Override
        public void start( long max )
        {
            this.max = max;
        }

        @Override
        public void progress( long add )
        {
            recoveredTransactions += add;
        }

        @Override
        public void completed()
        {
            completed = true;
        }

        public void verify()
        {
            assertTrue( "Progress reporting was not completed.", completed );
            assertEquals( "Number of max recovered transactions is different.", expectedMax, max );
            assertEquals( "Number of recovered transactions is different.", expectedMax, recoveredTransactions );
        }
    }

    private static class NextTransactionAnswer implements Answer<Boolean>
    {
        private final int expectedTransactionsToRecover;
        private int invocations;

        NextTransactionAnswer( int expectedTransactionsToRecover )
        {
            this.expectedTransactionsToRecover = expectedTransactionsToRecover;
        }

        @Override
        public Boolean answer( InvocationOnMock invocationOnMock )
        {
            invocations++;
            return invocations <= expectedTransactionsToRecover;
        }
    }
}
