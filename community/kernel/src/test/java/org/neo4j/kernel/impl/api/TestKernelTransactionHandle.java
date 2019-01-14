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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.locking.ActiveLock;

/**
 * A test implementation of {@link KernelTransactionHandle} that simply wraps a given {@link KernelTransaction}.
 */
public class TestKernelTransactionHandle implements KernelTransactionHandle
{
    private static final String USER_TRANSACTION_NAME_PREFIX = "transaction-";
    private final KernelTransaction tx;

    public TestKernelTransactionHandle( KernelTransaction tx )
    {
        this.tx = Objects.requireNonNull( tx );
    }

    @Override
    public long lastTransactionIdWhenStarted()
    {
        return tx.lastTransactionIdWhenStarted();
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        return tx.lastTransactionTimestampWhenStarted();
    }

    @Override
    public long startTime()
    {
        return tx.startTime();
    }

    @Override
    public long timeoutMillis()
    {
        return tx.timeout();
    }

    @Override
    public boolean isOpen()
    {
        return tx.isOpen();
    }

    @Override
    public boolean markForTermination( Status reason )
    {
        tx.markForTermination( reason );
        return true;
    }

    @Override
    public AuthSubject subject()
    {
        return tx.subjectOrAnonymous();
    }

    @Override
    public Map<String,Object> getMetaData()
    {
        return Collections.emptyMap();
    }

    @Override
    public Optional<Status> terminationReason()
    {
        return tx.getReasonIfTerminated();
    }

    @Override
    public boolean isUnderlyingTransaction( KernelTransaction tx )
    {
        return this.tx == tx;
    }

    @Override
    public long getUserTransactionId()
    {
        return tx.getTransactionId();
    }

    @Override
    public String getUserTransactionName()
    {
        return USER_TRANSACTION_NAME_PREFIX + getUserTransactionId();
    }

    @Override
    public Stream<ExecutingQuery> executingQueries()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return Stream.empty();
    }

    @Override
    public TransactionExecutionStatistic transactionStatistic()
    {
        return TransactionExecutionStatistic.NOT_AVAILABLE;
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
        TestKernelTransactionHandle that = (TestKernelTransactionHandle) o;
        return tx.equals( that.tx );
    }

    @Override
    public int hashCode()
    {
        return tx.hashCode();
    }

    @Override
    public String toString()
    {
        return "TestKernelTransactionHandle{tx=" + tx + "}";
    }
}
