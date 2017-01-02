/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.guard;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactionTestBase;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class TimeoutGuardTest extends KernelTransactionTestBase
{

    private AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Before
    public void setUp()
    {
        logProvider.clear();
    }

    @Test
    public void detectTimedTransaction()
    {
        long transactionTimeout = 10L;
        long overtime = 1L;
        TimeoutGuard timeoutGuard = buildGuard( logProvider );
        initClock();

        KernelStatement kernelStatement = getKernelStatement( transactionTimeout );
        clock.forward( transactionTimeout + overtime, TimeUnit.MILLISECONDS );
        String message = "Transaction timeout. (Overtime: 1 ms).";

        check( timeoutGuard, kernelStatement, overtime, message );

        KernelTransactionImplementation transaction = kernelStatement.getTransaction();
        assertSame( Status.Transaction.TransactionTimedOut, transaction.getReasonIfTerminated().get() );

        logProvider.assertContainsMessageContaining( message );
    }

    @Test
    public void allowToProceedWhenTransactionTimeoutNotReached()
    {
        long transactionTimeout = 100000L;
        long overtime = 5L;
        TimeoutGuard timeoutGuard = buildGuard( logProvider );
        initClock();

        KernelStatement kernelStatement = getKernelStatement( transactionTimeout );
        clock.forward( overtime, TimeUnit.MILLISECONDS );

        timeoutGuard.check( kernelStatement );

        KernelTransactionImplementation transaction = kernelStatement.getTransaction();
        assertFalse( transaction.getReasonIfTerminated().isPresent() );

        logProvider.assertNoLoggingOccurred();
    }

    private void initClock()
    {
        long startTime = getStartTime();
        clock.forward( startTime, TimeUnit.MILLISECONDS );
    }

    private void check( TimeoutGuard timeoutGuard, KernelStatement kernelStatement, long overtime, String message )
    {
        try
        {
            timeoutGuard.check( kernelStatement );
        }
        catch ( GuardTimeoutException e )
        {
            assertEquals( "Exception should have expected message.", message, e.getMessage() );
            assertEquals( "Exception should have correct overtime value.", overtime, e.getOvertime() );
        }
    }

    private KernelStatement getKernelStatement( long transactionTimeout )
    {
        KernelTransactionImplementation transaction = newNotInitializedTransaction();
        StatementLocks statementLocks = mock( StatementLocks.class );
        transaction.initialize( 1L, 2L, statementLocks, KernelTransaction.Type.implicit,
                AUTH_DISABLED, transactionTimeout );
        return transaction.acquireStatement();
    }

    private TimeoutGuard buildGuard( AssertableLogProvider logProvider )
    {
        Log log = logProvider.getLog( TimeoutGuard.class );
        return new TimeoutGuard( clock, log );
    }

    private long getStartTime()
    {
        LocalDateTime startTime = LocalDateTime.of( 2016, Month.AUGUST, 17, 15, 10 );
        return startTime.toInstant( ZoneOffset.UTC ).toEpochMilli();
    }
}
