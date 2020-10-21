/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api.transaction.monitor;

import java.util.Set;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

/**
 * Transaction monitor that check transactions with a configured timeout for expiration.
 * In case if transaction timed out it will be terminated.
 */
public abstract class TransactionMonitor implements Runnable
{
    private final SystemNanoClock clock;
    private final Log log;

    public TransactionMonitor( SystemNanoClock clock, LogService logService )
    {
        this.clock = clock;
        this.log = logService.getInternalLog( TransactionMonitor.class );
    }

    @Override
    public void run()
    {
        long nowNanos = clock.nanos();
        Set<MonitoredTransaction> activeTransactions = getActiveTransactions();
        checkExpiredTransactions( activeTransactions, nowNanos );
    }

    protected abstract Set<MonitoredTransaction> getActiveTransactions();

    private void checkExpiredTransactions( Set<MonitoredTransaction> activeTransactions, long nowNanos )
    {
        for ( MonitoredTransaction activeTransaction : activeTransactions )
        {
            long transactionTimeoutNanos = activeTransaction.timeoutNanos();
            if ( transactionTimeoutNanos > 0 )
            {
                if ( isTransactionExpired( activeTransaction, nowNanos, transactionTimeoutNanos ) && !activeTransaction.isSchemaTransaction() )
                {
                    if ( activeTransaction.markForTermination( Status.Transaction.TransactionTimedOut ) )
                    {
                        log.warn( "Transaction %s timeout.", activeTransaction.getIdentifyingDescription() );
                    }
                }
            }
        }
    }

    private static boolean isTransactionExpired( MonitoredTransaction activeTransaction, long nowNanos, long transactionTimeoutNanos )
    {
        return nowNanos - activeTransaction.startTimeNanos() > transactionTimeoutNanos;
    }

    public interface MonitoredTransaction
    {
        long startTimeNanos();

        long timeoutNanos();

        boolean isSchemaTransaction();

        /**
         * Mark the underlying transaction for termination.
         *
         * @param reason the reason for termination.
         * @return {@code true} if the underlying transaction was marked for termination, {@code false} otherwise
         * (when this handle represents an old transaction that has been closed).
         */
        boolean markForTermination( Status reason );

        /**
         * A meaningful description used in log messages related to this transaction.
         * <p>
         * In other words, this is meant to be a user-facing 'toString' containing
         * information that can help the reader of the log to identify the transaction
         * that timed out.
         */
        String getIdentifyingDescription();
    }
}
