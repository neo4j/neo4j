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
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.time.SystemNanoClock;

/**
 * A {@link KernelTransactionHandle} that wraps the given {@link KernelTransactionImplementation}.
 * This handle knows that {@link KernelTransactionImplementation}s can be reused and represents a single logical
 * transaction. This means that methods like {@link #markForTermination(Status)} can only terminate running
 * transaction this handle was created for.
 */
class KernelTransactionImplementationHandle implements KernelTransactionHandle
{
    private static final String USER_TRANSACTION_NAME_SEPARATOR = "-transaction-";

    private final long txReuseCount;
    private final long lastTransactionTimestampWhenStarted;
    private final long startTime;
    private final long startTimeNanos;
    private final long timeoutMillis;
    private final KernelTransactionImplementation tx;
    private final SystemNanoClock clock;
    private final ClientConnectionInfo clientInfo;
    private final AuthSubject subject;
    private final Optional<Status> terminationReason;
    private final Optional<ExecutingQuery> executingQuery;
    private final Map<String,Object> metaData;
    private final long userTransactionId;
    private final TransactionInitializationTrace initializationTrace;
    private final String databaseName;

    KernelTransactionImplementationHandle( KernelTransactionImplementation tx, SystemNanoClock clock )
    {
        this.txReuseCount = tx.getReuseCount();
        this.lastTransactionTimestampWhenStarted = tx.lastTransactionTimestampWhenStarted();
        this.startTime = tx.startTime();
        this.startTimeNanos = tx.startTimeNanos();
        this.timeoutMillis = tx.timeout();
        this.subject = tx.subjectOrAnonymous();
        this.terminationReason = tx.getReasonIfTerminated();
        this.executingQuery = tx.executingQuery();
        this.metaData = tx.getMetaData();
        this.userTransactionId = tx.userTransactionId();
        this.initializationTrace = tx.getInitializationTrace();
        this.clientInfo = tx.clientInfo();
        databaseName = tx.getDatabaseName();
        this.tx = tx;
        this.clock = clock;
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        return lastTransactionTimestampWhenStarted;
    }

    @Override
    public long startTime()
    {
        return startTime;
    }

    @Override
    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    @Override
    public long timeoutMillis()
    {
        return timeoutMillis;
    }

    @Override
    public boolean isOpen()
    {
        return tx.isOpen() && txReuseCount == tx.getReuseCount();
    }

    @Override
    public boolean isClosing()
    {
        return tx.isClosing() && txReuseCount == tx.getReuseCount();
    }

    @Override
    public boolean markForTermination( Status reason )
    {
        return tx.markForTermination( txReuseCount, reason );
    }

    @Override
    public AuthSubject subject()
    {
        return subject;
    }

    @Override
    public Map<String,Object> getMetaData()
    {
        return metaData;
    }

    @Override
    public Optional<Status> terminationReason()
    {
        return terminationReason;
    }

    @Override
    public boolean isUnderlyingTransaction( KernelTransaction tx )
    {
        return this.tx == tx;
    }

    @Override
    public long getUserTransactionId()
    {
        return userTransactionId;
    }

    @Override
    public String getUserTransactionName()
    {
        return databaseName + USER_TRANSACTION_NAME_SEPARATOR + getUserTransactionId();
    }

    @Override
    public Optional<ExecutingQuery> executingQuery()
    {
        return executingQuery;
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return tx.activeLocks();
    }

    @Override
    public TransactionExecutionStatistic transactionStatistic()
    {
        if ( txReuseCount == tx.getReuseCount() )
        {
            return new TransactionExecutionStatistic( tx, clock, startTime );
        }
        else
        {
            return TransactionExecutionStatistic.NOT_AVAILABLE;
        }
    }

    @Override
    public TransactionInitializationTrace transactionInitialisationTrace()
    {
        return initializationTrace;
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        return clientInfo;
    }

    @Override
    public boolean isSchemaTransaction()
    {
        return tx.isSchemaTransaction();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        KernelTransactionImplementationHandle that = (KernelTransactionImplementationHandle) o;
        return txReuseCount == that.txReuseCount && tx.equals( that.tx );
    }

    @Override
    public int hashCode()
    {
        return 31 * (int) (txReuseCount ^ (txReuseCount >>> 32)) + tx.hashCode();
    }

    @Override
    public String toString()
    {
        return "KernelTransactionImplementationHandle{txReuseCount=" + txReuseCount + ", tx=" + tx + '}';
    }
}
