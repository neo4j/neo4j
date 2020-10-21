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
package org.neo4j.fabric.transaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class FabricTransactionMonitor extends TransactionMonitor
{
    private final Map<FabricTransactionImpl,MonitoredTransaction> transactions = new ConcurrentHashMap<>();
    private final SystemNanoClock clock;
    private final FabricConfig fabricConfig;

    public FabricTransactionMonitor( SystemNanoClock clock, LogService logService, FabricConfig fabricConfig )
    {
        super( clock, logService );

        this.clock = clock;
        this.fabricConfig = fabricConfig;
    }

    public void startMonitoringTransaction( FabricTransactionImpl transaction, FabricTransactionInfo transactionInfo )
    {
        long startTimeNanos = clock.nanos();
        long timeoutNanos;
        if ( transactionInfo.getTxTimeout() != null )
        {
            timeoutNanos = transactionInfo.getTxTimeout().toNanos();
        }
        else
        {
            timeoutNanos = fabricConfig.getTransactionTimeout().toNanos();
        }

        transactions.put( transaction, new FabricMonitoredTransaction( transaction, startTimeNanos, timeoutNanos ) );
    }

    public void stopMonitoringTransaction( FabricTransactionImpl transaction )
    {
        transactions.remove( transaction );
    }

    @Override
    protected Set<MonitoredTransaction> getActiveTransactions()
    {
        return new HashSet<>( transactions.values() );
    }

    private static class FabricMonitoredTransaction implements MonitoredTransaction
    {
        private final FabricTransactionImpl fabricTransaction;
        private final long startTimeNanos;
        private final long timeoutNanos;

        FabricMonitoredTransaction( FabricTransactionImpl fabricTransaction, long startTimeNanos, long timeoutNanos )
        {
            this.fabricTransaction = fabricTransaction;
            this.startTimeNanos = startTimeNanos;
            this.timeoutNanos = timeoutNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return startTimeNanos;
        }

        @Override
        public long timeoutNanos()
        {
            return timeoutNanos;
        }

        @Override
        public boolean isSchemaTransaction()
        {
            return fabricTransaction.isSchemaTransaction();
        }

        @Override
        public boolean markForTermination( Status reason )
        {
            fabricTransaction.markForTermination( reason );
            return true;
        }

        @Override
        public String getIdentifyingDescription()
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "QueryRouterTransaction[" );
            sb.append( "id=" ).append( fabricTransaction.getId() ).append( "," );

            var rawAddress = fabricTransaction.getTransactionInfo().getClientConnectionInfo().clientAddress();
            var address = rawAddress == null ? "embedded" : rawAddress;
            sb.append( "clientAddress=" ).append( address );
            var authSubject = fabricTransaction.getTransactionInfo().getLoginContext().subject();
            if ( authSubject != AuthSubject.ANONYMOUS && authSubject != AuthSubject.AUTH_DISABLED )
            {
                sb.append( "," ).append( "username=" ).append( authSubject.username() );
            }

            sb.append( "]" );
            return sb.toString();
        }
    }
}
