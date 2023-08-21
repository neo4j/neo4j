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

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ImpermanentDbmsExtension
@ExtendWith(RandomExtension.class)
public class TransactionStoreConcurrentIT {

    public static final int TX_COUNT = 1000;

    @Inject
    private GraphDatabaseService db;

    @Inject
    private GraphDatabaseAPI dbapi;

    @Inject
    private RandomSupport random;

    @BeforeEach
    void before() {
        for (int i = 0; i < TX_COUNT; i++) {
            createTx();
        }
    }

    @RepeatedTest(10)
    void testConcurentScanOfTransactionLog() throws Throwable {
        var race = new Race();
        race.addContestants(100, this::doThings);
        race.addContestant(this::createTx, 100);
        race.go();
    }

    private void doThings() {
        try {
            var logicalTransactionStore =
                    dbapi.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
            try (var txCursor = logicalTransactionStore.getCommandBatches(random.nextLong(TX_COUNT / 2, TX_COUNT))) {
                while (txCursor.next()) {
                    txCursor.position();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(Thread.currentThread().getName(), e);
        }
    }

    private void createTx() {
        try (var tx = db.beginTx()) {
            tx.createNode().setProperty("test", "Test");
            tx.commit();
        }
    }
}
