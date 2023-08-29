/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import static java.util.Optional.ofNullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.lock.ActiveLock;

/**
 * A test implementation of {@link KernelTransactionHandle} that simply wraps a given {@link KernelTransaction}.
 */
public class TestKernelTransactionHandle implements KernelTransactionHandle {
    private static final String USER_TRANSACTION_NAME_PREFIX = "transaction-";
    private final KernelTransaction tx;

    public TestKernelTransactionHandle(KernelTransaction tx) {
        this.tx = Objects.requireNonNull(tx);
    }

    @Override
    public long startTime() {
        return tx.startTime();
    }

    @Override
    public long startTimeNanos() {
        return tx.startTimeNanos();
    }

    @Override
    public TransactionTimeout timeout() {
        return tx.timeout();
    }

    @Override
    public boolean isOpen() {
        return tx.isOpen();
    }

    @Override
    public boolean isClosing() {
        return tx.isClosing();
    }

    @Override
    public boolean isCommitting() {
        return tx.isCommitting();
    }

    @Override
    public boolean isRollingback() {
        return tx.isRollingback();
    }

    @Override
    public boolean markForTermination(Status reason) {
        tx.markForTermination(reason);
        return true;
    }

    @Override
    public AuthSubject subject() {
        return tx.subjectOrAnonymous();
    }

    @Override
    public Map<String, Object> getMetaData() {
        return Collections.emptyMap();
    }

    @Override
    public Optional<TerminationMark> terminationMark() {
        return tx.getTerminationMark();
    }

    @Override
    public boolean isUnderlyingTransaction(KernelTransaction tx) {
        return this.tx == tx;
    }

    @Override
    public long getTransactionSequenceNumber() {
        return tx.getTransactionSequenceNumber();
    }

    @Override
    public String getUserTransactionName() {
        return USER_TRANSACTION_NAME_PREFIX + getTransactionSequenceNumber();
    }

    @Override
    public Optional<ExecutingQuery> executingQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ActiveLock> activeLocks() {
        return Collections.emptyList();
    }

    @Override
    public TransactionExecutionStatistic transactionStatistic() {
        return TransactionExecutionStatistic.NOT_AVAILABLE;
    }

    @Override
    public TransactionInitializationTrace transactionInitialisationTrace() {
        return TransactionInitializationTrace.NONE;
    }

    @Override
    public Optional<ClientConnectionInfo> clientInfo() {
        return ofNullable(tx.clientInfo());
    }

    @Override
    public boolean isSchemaTransaction() {
        return tx.isSchemaTransaction();
    }

    @Override
    public String getStatusDetails() {
        return tx.statusDetails();
    }

    @Override
    public long getLastClosedTxId() {
        return tx.cursorContext().getVersionContext().lastClosedTransactionId();
    }

    @Override
    public long getTransactionHorizon() {
        return tx.cursorContext().getVersionContext().oldestVisibleTransactionNumber();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestKernelTransactionHandle that = (TestKernelTransactionHandle) o;
        return tx.equals(that.tx);
    }

    @Override
    public int hashCode() {
        return tx.hashCode();
    }

    @Override
    public String toString() {
        return "TestKernelTransactionHandle{tx=" + tx + "}";
    }
}
