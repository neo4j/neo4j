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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;


public class KernelTransactionMonitor extends TransactionMonitor
{
    private final KernelTransactions kernelTransactions;

    public KernelTransactionMonitor( KernelTransactions kernelTransactions, SystemNanoClock clock, LogService logService )
    {
        super( clock, logService );
        this.kernelTransactions = kernelTransactions;
    }

    @Override
    protected Set<MonitoredTransaction> getActiveTransactions()
    {
        return kernelTransactions.activeTransactions()
                                 .stream()
                                 .map( MonitoredKernelTransaction::new )
                                 .collect( Collectors.toSet() );
    }

    private static class MonitoredKernelTransaction implements MonitoredTransaction
    {
        private final KernelTransactionHandle kernelTransaction;

        MonitoredKernelTransaction( KernelTransactionHandle kernelTransaction )
        {
            this.kernelTransaction = kernelTransaction;
        }

        @Override
        public long startTimeNanos()
        {
            return kernelTransaction.startTimeNanos();
        }

        @Override
        public long timeoutNanos()
        {
            return TimeUnit.MILLISECONDS.toNanos(kernelTransaction.timeoutMillis());
        }

        @Override
        public boolean isSchemaTransaction()
        {
            return kernelTransaction.isSchemaTransaction();
        }

        @Override
        public boolean markForTermination( Status reason )
        {
            return kernelTransaction.markForTermination( reason );
        }

        @Override
        public String getIdentifyingDescription()
        {
            // this is a legacy implementation, so let's use
            // 'toString' on KernelTransactionHandle which was used for years for this purpose
            return kernelTransaction.toString();
        }
    }
}
