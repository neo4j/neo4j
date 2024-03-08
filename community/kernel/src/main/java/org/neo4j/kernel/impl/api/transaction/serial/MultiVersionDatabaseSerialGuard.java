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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

public class MultiVersionDatabaseSerialGuard implements DatabaseSerialGuard {
    private final AtomicLong serialExecutionStamp = new AtomicLong();
    private final Set<KernelTransactionImplementation> allTransactions;

    public MultiVersionDatabaseSerialGuard(Set<KernelTransactionImplementation> allTransactions) {
        this.allTransactions = allTransactions;
    }

    @Override
    public long serialExecution() {
        return serialExecutionStamp.getAcquire();
    }

    @Override
    public void acquireSerialLock(KernelTransaction ktx) {
        if (KernelTransaction.Type.SERIAL != ktx.transactionType()) {
            return;
        }
        long sequenceNumber = ktx.getTransactionSequenceNumber();
        while (!serialExecutionStamp.weakCompareAndSetRelease(0, sequenceNumber)) {
            LockSupport.parkNanos(100);
        }

        // now new transaction are not able to start writes
        // we start waiting for all transactions that already started to finish writes to complete
        while (!oldTransactionCompleted(sequenceNumber)) {
            LockSupport.parkNanos(100);
        }

        // refresh visibility context
        ktx.cursorContext().getVersionContext().initRead();
    }

    @Override
    public void releaseSerialLock() {
        serialExecutionStamp.setRelease(0);
    }

    private boolean oldTransactionCompleted(long currentValue) {
        for (KernelTransactionImplementation transaction : allTransactions) {
            if (transaction.isDataTransaction() || transaction.isSchemaTransaction()) {
                long sequenceNumber = transaction.getTransactionSequenceNumber();
                if (sequenceNumber != 0 && sequenceNumber < currentValue) {
                    return false;
                }
            }
        }
        return true;
    }
}
