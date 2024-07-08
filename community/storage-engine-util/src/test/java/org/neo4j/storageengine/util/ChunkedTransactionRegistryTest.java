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
package org.neo4j.storageengine.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.OpenTransactionMetadata;

class ChunkedTransactionRegistryTest {

    private ChunkedTransactionRegistry transactionRegistry;

    @BeforeEach
    void setUp() {
        transactionRegistry = new ChunkedTransactionRegistry();
    }

    @Test
    void removeNotExistingTransaction() {
        for (int i = 0; i < 10; i++) {
            long txId = i;
            assertDoesNotThrow(() -> transactionRegistry.removeTransaction(txId));
        }
    }

    @Test
    void emptyRegistryOldestOpenInfo() {
        assertNull(transactionRegistry.oldestOpenTransactionMetadata());
    }

    @Test
    void oldestRegisteredOpenTransactionInfo() {
        transactionRegistry.registerTransaction(4, 44, new LogPosition(0, 4));
        transactionRegistry.registerTransaction(1, 11, new LogPosition(0, 1));
        transactionRegistry.registerTransaction(2, 22, new LogPosition(0, 2));
        transactionRegistry.registerTransaction(3, 33, new LogPosition(0, 3));

        assertEquals(
                new OpenTransactionMetadata(1, 11, new LogPosition(0, 1)),
                transactionRegistry.oldestOpenTransactionMetadata());

        transactionRegistry.removeTransaction(1);
        assertEquals(
                new OpenTransactionMetadata(2, 22, new LogPosition(0, 2)),
                transactionRegistry.oldestOpenTransactionMetadata());

        transactionRegistry.removeTransaction(2);
        assertEquals(
                new OpenTransactionMetadata(3, 33, new LogPosition(0, 3)),
                transactionRegistry.oldestOpenTransactionMetadata());

        transactionRegistry.removeTransaction(3);
        assertEquals(
                new OpenTransactionMetadata(4, 44, new LogPosition(0, 4)),
                transactionRegistry.oldestOpenTransactionMetadata());

        transactionRegistry.removeTransaction(4);
        assertNull(transactionRegistry.oldestOpenTransactionMetadata());
    }

    @Test
    void multiThreadedTransactionRegistryOperations() throws InterruptedException {
        int numberOfWorkers = 10;
        var transactionReporters = Executors.newFixedThreadPool(numberOfWorkers);
        try {
            var registrationLatch = new CountDownLatch(numberOfWorkers);
            var deletionLatch = new CountDownLatch(numberOfWorkers / 2);
            for (int i = 0; i < numberOfWorkers; i++) {
                long txId = i + 1;

                transactionReporters.submit(() -> {
                    transactionRegistry.registerTransaction(txId, txId + 100, new LogPosition(txId, 100));
                    registrationLatch.countDown();
                });
            }

            registrationLatch.await();
            assertEquals(
                    new OpenTransactionMetadata(1, 101, new LogPosition(1, 100)),
                    transactionRegistry.oldestOpenTransactionMetadata());

            for (int i = 0; i < numberOfWorkers / 2; i++) {
                long txId = i + 1;

                transactionReporters.submit(() -> {
                    transactionRegistry.removeTransaction(txId);
                    deletionLatch.countDown();
                });
            }

            deletionLatch.await();
            assertEquals(
                    new OpenTransactionMetadata(6, 106, new LogPosition(6, 100)),
                    transactionRegistry.oldestOpenTransactionMetadata());
        } finally {
            transactionReporters.shutdown();
        }
    }
}
