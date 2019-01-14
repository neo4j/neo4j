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

import java.time.Clock;
import java.util.Set;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

/**
 * Transaction monitor that check transactions with a configured timeout for expiration.
 * In case if transaction timed out it will be terminated.
 */
public class KernelTransactionTimeoutMonitor implements Runnable
{
    private final KernelTransactions kernelTransactions;
    private final Clock clock;
    private final Log log;

    public KernelTransactionTimeoutMonitor( KernelTransactions kernelTransactions, Clock clock, LogService logService )
    {
        this.kernelTransactions = kernelTransactions;
        this.clock = clock;
        this.log = logService.getInternalLog( KernelTransactionTimeoutMonitor.class );
    }

    @Override
    public synchronized void run()
    {
        Set<KernelTransactionHandle> activeTransactions = kernelTransactions.activeTransactions();
        long now = clock.millis();
        for ( KernelTransactionHandle activeTransaction : activeTransactions )
        {
            long transactionTimeoutMillis = activeTransaction.timeoutMillis();
            if ( transactionTimeoutMillis > 0 )
            {
                if ( isTransactionExpired( activeTransaction, now, transactionTimeoutMillis ) )
                {
                    if ( activeTransaction.markForTermination( Status.Transaction.TransactionTimedOut ) )
                    {
                        log.warn( "Transaction %s timeout.", activeTransaction );
                    }
                }
            }
        }
    }

    private boolean isTransactionExpired( KernelTransactionHandle activeTransaction, long nowMillis,
            long transactionTimeoutMillis )
    {
        return nowMillis > (activeTransaction.startTime() + transactionTimeoutMillis);
    }
}
