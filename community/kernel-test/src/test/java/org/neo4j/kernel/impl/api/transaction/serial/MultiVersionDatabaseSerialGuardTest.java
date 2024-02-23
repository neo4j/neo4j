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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

public class MultiVersionDatabaseSerialGuardTest {
    @Test
    void acquireLockForSerialTransaction() {
        var serialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());

        var nonSerialTransaction = createNonSerialTransaction();

        var serialTransaction = createSerialTransaction();

        serialGuard.acquireSerialLock(nonSerialTransaction);
        assertEquals(DatabaseSerialGuard.EMPTY_STAMP, serialGuard.serialExecution());

        serialGuard.acquireSerialLock(serialTransaction);
        assertEquals(3L, serialGuard.serialExecution());
    }

    @Test
    void releaseSerialLockWhenLocked() {
        var serialGuard = new MultiVersionDatabaseSerialGuard(ConcurrentHashMap.newKeySet());

        var serialTransaction = createSerialTransaction();

        serialGuard.acquireSerialLock(serialTransaction);
        assertEquals(3L, serialGuard.serialExecution());

        serialGuard.releaseSerialLock();

        assertEquals(DatabaseSerialGuard.EMPTY_STAMP, serialGuard.serialExecution());
    }

    private static KernelTransactionImplementation createNonSerialTransaction() {
        var nonSerialTransaction = mock(KernelTransactionImplementation.class);
        when(nonSerialTransaction.transactionType()).thenReturn(KernelTransaction.Type.EXPLICIT);
        when(nonSerialTransaction.getTransactionSequenceNumber()).thenReturn(2L);
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
