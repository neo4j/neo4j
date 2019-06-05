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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaciton.monitor.KernelTransactionMonitor;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KernelTransactionTimeoutMonitorTest
{
    private static final int EXPECTED_REUSE_COUNT = 2;
    private KernelTransactions kernelTransactions;
    private FakeClock fakeClock;
    private AssertableLogProvider logProvider;
    private LogService logService;

    @BeforeEach
    void setUp()
    {
        kernelTransactions = mock( KernelTransactions.class );
        fakeClock = Clocks.fakeClock();
        logProvider = new AssertableLogProvider();
        logService = new SimpleLogService( logProvider, logProvider );
    }

    @Test
    void terminateExpiredTransactions()
    {
        HashSet<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock( 3, 1, 3 );
        KernelTransactionImplementation tx2 = prepareTxMock( 4, 1, 8 );
        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle( tx1, fakeClock );
        KernelTransactionImplementationHandle handle2 = new KernelTransactionImplementationHandle( tx2, fakeClock );
        transactions.add( handle1 );
        transactions.add( handle2 );

        when( kernelTransactions.activeTransactions()).thenReturn( transactions );

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor();

        fakeClock.forward( 3, TimeUnit.MILLISECONDS );
        transactionMonitor.run();

        verify( tx1, never() ).markForTermination( Status.Transaction.TransactionTimedOut );
        verify( tx2, never() ).markForTermination( Status.Transaction.TransactionTimedOut );
        logProvider.rawMessageMatcher().assertNotContains( "timeout" );

        fakeClock.forward( 2, TimeUnit.MILLISECONDS );
        transactionMonitor.run();

        verify( tx1 ).markForTermination( EXPECTED_REUSE_COUNT, Status.Transaction.TransactionTimedOut );
        verify( tx2, never() ).markForTermination( Status.Transaction.TransactionTimedOut );
        logProvider.rawMessageMatcher().assertContains( "timeout" );

        logProvider.clear();
        fakeClock.forward( 10, TimeUnit.MILLISECONDS );
        transactionMonitor.run();

        verify( tx2 ).markForTermination( EXPECTED_REUSE_COUNT, Status.Transaction.TransactionTimedOut );
        logProvider.rawMessageMatcher().assertContains( "timeout" );
    }

    @Test
    void skipTransactionWithoutTimeout()
    {
        HashSet<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock( 7, 3, 0 );
        KernelTransactionImplementation tx2 = prepareTxMock( 8, 4, 0 );
        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle( tx1, fakeClock );
        KernelTransactionImplementationHandle handle2 = new KernelTransactionImplementationHandle( tx2, fakeClock );
        transactions.add( handle1 );
        transactions.add( handle2 );

        when( kernelTransactions.activeTransactions()).thenReturn( transactions );

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor();

        fakeClock.forward( 300, TimeUnit.MILLISECONDS );
        transactionMonitor.run();

        verify( tx1, never() ).markForTermination( Status.Transaction.TransactionTimedOut );
        verify( tx2, never() ).markForTermination( Status.Transaction.TransactionTimedOut );
        logProvider.rawMessageMatcher().assertNotContains( "timeout" );
    }

    private KernelTransactionMonitor buildTransactionMonitor()
    {
        return new KernelTransactionMonitor( kernelTransactions, fakeClock, logService );
    }

    private static KernelTransactionImplementation prepareTxMock( long userTxId, long startMillis, long timeoutMillis )
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.startTime() ).thenReturn( startMillis );
        when( transaction.userTransactionId() ).thenReturn( userTxId );
        when( transaction.getReuseCount() ).thenReturn( EXPECTED_REUSE_COUNT );
        when( transaction.timeout() ).thenReturn( timeoutMillis );
        when( transaction.markForTermination( EXPECTED_REUSE_COUNT, Status.Transaction.TransactionTimedOut ) ).thenReturn( true );
        return transaction;
    }
}
