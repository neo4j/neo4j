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

import static org.neo4j.kernel.api.KernelTransaction.Type.SERIAL;
import static org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard.EMPTY_STAMP;

import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;

public final class TransactionSerialExecutionGuard implements SerialExecutionGuard {
    private final DatabaseSerialGuard databaseSerialGuard;
    private final KernelTransaction transaction;

    public TransactionSerialExecutionGuard(DatabaseSerialGuard databaseSerialGuard, KernelTransaction transaction) {
        this.databaseSerialGuard = databaseSerialGuard;
        this.transaction = transaction;
    }

    @Override
    public void check() {
        long serialExecution = databaseSerialGuard.serialExecution();
        if (EMPTY_STAMP == serialExecution) {
            return;
        }

        if (SERIAL != transaction.transactionType() && transaction.getTransactionSequenceNumber() >= serialExecution) {
            throw new TransientTransactionFailureException(
                    Status.Transaction.TransactionValidationFailed,
                    "Serial transaction execution enforcement guard. Serial transaction sequence number: "
                            + serialExecution + " enforced termination of transaction with sequence number: "
                            + transaction.getTransactionSequenceNumber());
        }
    }

    @Override
    public void release() {
        if (SERIAL == transaction.transactionType()) {
            databaseSerialGuard.releaseSerialLock();
        }
    }
}
