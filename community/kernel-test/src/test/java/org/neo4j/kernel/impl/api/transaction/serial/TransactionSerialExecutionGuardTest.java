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
package org.neo4j.kernel.impl.api.transaction.serial;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

public class TransactionSerialExecutionGuardTest {
    @Test
    void nonSerialTransactionCheckOnNonLockedGlobalGuard() {
        var databaseSerialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());
        var nonSerialTransaction = createNotSerialTransaction(2);
        var executionGuard = new TransactionSerialExecutionGuard(databaseSerialGuard, nonSerialTransaction);

        assertDoesNotThrow(executionGuard::check);
    }

    @Test
    void nonSerialTransactionCheckOnLockedGlobalGuard() {
        var databaseSerialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());
        databaseSerialGuard.acquireSerialLock(createSerialTransaction());
        var nonSerialTransaction = createNotSerialTransaction(5);
        var executionGuard = new TransactionSerialExecutionGuard(databaseSerialGuard, nonSerialTransaction);

        assertThatThrownBy(executionGuard::check).isInstanceOf(TransientTransactionFailureException.class);
    }

    @Test
    void serialTransactionCheckOnLockedGlobalGuard() {
        var databaseSerialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());
        var serialTransaction = createSerialTransaction();
        databaseSerialGuard.acquireSerialLock(serialTransaction);
        var executionGuard = new TransactionSerialExecutionGuard(databaseSerialGuard, serialTransaction);

        assertDoesNotThrow(executionGuard::check);
    }

    @Test
    void nonSerialTransactionCheckOnLockedGlobalGuardStartedBefore() {
        var databaseSerialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());
        databaseSerialGuard.acquireSerialLock(createSerialTransaction());
        var nonSerialTransaction = createNotSerialTransaction(2);
        var executionGuard = new TransactionSerialExecutionGuard(databaseSerialGuard, nonSerialTransaction);

        assertDoesNotThrow(executionGuard::check);
    }

    private static KernelTransactionImplementation createNotSerialTransaction(long sequenceNumber) {
        var nonSerialTransaction = mock(KernelTransactionImplementation.class);
        when(nonSerialTransaction.transactionType()).thenReturn(KernelTransaction.Type.EXPLICIT);
        when(nonSerialTransaction.getTransactionSequenceNumber()).thenReturn(sequenceNumber);
        return nonSerialTransaction;
    }

    private static KernelTransactionImplementation createSerialTransaction() {
        var serialTransaction = mock(KernelTransactionImplementation.class);
        when(serialTransaction.transactionType()).thenReturn(KernelTransaction.Type.SERIAL);
        when(serialTransaction.getTransactionSequenceNumber()).thenReturn(3L);
        when(serialTransaction.cursorContext()).thenReturn(CursorContext.NULL_CONTEXT);
        return serialTransaction;
    }
}
