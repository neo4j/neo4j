/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.function.Supplier;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.logging.Log;

public class TimeoutGuard implements Guard
{
    private final Log log;
    private Clock clock;

    public TimeoutGuard( final Log log, Clock clock )
    {
        this.log = log;
        this.clock = clock;
    }

    @Override
    public void check( KernelStatement statement )
    {
        check( maxStatementCompletionTimeSupplier( statement ), "Statement timeout." );
        check( statement.getTransaction() );
    }

    private void check (KernelTransactionImplementation transaction)
    {
        check( maxTransactionCompletionTimeSupplier( transaction ), "Transaction timeout." );
    }

    private void check( Supplier<Long> completionTimeSupplier, String timeoutDescription )
    {
        long now = clock.currentTimeMillis();
        long transactionCompletionTime = completionTimeSupplier.get();
        if ( transactionCompletionTime < now )
        {
            final long overtime = now - transactionCompletionTime;
            log.warn( timeoutDescription + " ( Overtime: " + overtime + " ms)." );
            throw new GuardTimeoutException( overtime );
        }
    }

    private static Supplier<Long> maxTransactionCompletionTimeSupplier( KernelTransactionImplementation transaction )
    {
        return () -> getMaxTransactionCompletionTime( transaction );
    }

    private static Supplier<Long> maxStatementCompletionTimeSupplier( KernelStatement statement )
    {
        return () -> getMaxStatementCompletionTime( statement );
    }

    private static long getMaxStatementCompletionTime( KernelStatement statement )
    {
        return statement.startTime() + statement.timeout();
    }

    private static long getMaxTransactionCompletionTime( KernelTransactionImplementation transaction )
    {
        return transaction.startTime() + transaction.timeout();
    }
}
