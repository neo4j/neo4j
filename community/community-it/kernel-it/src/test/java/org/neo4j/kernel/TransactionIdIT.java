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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdController.TransactionSnapshot;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionIdSequence;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class TransactionIdIT {
    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Test
    void sequenceNumberIncreasing() {
        var dependencyResolver = databaseAPI.getDependencyResolver();
        var transactionIdSequence = dependencyResolver.resolveDependency(TransactionIdSequence.class);
        long currentBase = transactionIdSequence.currentValue();
        for (int i = 0; i < 100; i++) {
            try (TransactionImpl transaction = (TransactionImpl) databaseAPI.beginTx()) {
                long sequenceNumber = transaction.kernelTransaction().getTransactionSequenceNumber();
                assertThat(sequenceNumber).isGreaterThan(currentBase);
                currentBase = sequenceNumber;
            }
        }
    }

    @Test
    void transactionReuseCriteriaBasedOnSequenceNumber() {
        var dependencyResolver = databaseAPI.getDependencyResolver();
        var kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        TransactionSnapshot snapshot;
        try (TransactionImpl oldTransaction = (TransactionImpl) databaseAPI.beginTx()) {
            long sequenceNumber = oldTransaction.kernelTransaction().getTransactionSequenceNumber();
            snapshot = new TransactionSnapshot(sequenceNumber, 1, 1);
            assertFalse(kernelTransactions.eligibleForFreeing(snapshot));
            for (int i = 0; i < 100; i++) {
                //noinspection EmptyTryBlock
                try (var transaction = databaseAPI.beginTx()) {}
            }
            assertFalse(kernelTransactions.eligibleForFreeing(snapshot));
        }
        assertTrue(kernelTransactions.eligibleForFreeing(snapshot));
    }

    @Test
    void oldestActiveTransactionLookup() {
        var dependencyResolver = databaseAPI.getDependencyResolver();
        var kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        long sequenceNumber;
        try (TransactionImpl oldTransaction = (TransactionImpl) databaseAPI.beginTx()) {
            sequenceNumber = oldTransaction.kernelTransaction().getTransactionSequenceNumber();
            for (int i = 0; i < 100; i++) {
                //noinspection EmptyTryBlock
                try (var transaction = databaseAPI.beginTx()) {}
            }
            assertEquals(sequenceNumber, kernelTransactions.oldestActiveTransactionSequenceNumber());
        }
        assertNotEquals(sequenceNumber, kernelTransactions.oldestActiveTransactionSequenceNumber());
    }
}
