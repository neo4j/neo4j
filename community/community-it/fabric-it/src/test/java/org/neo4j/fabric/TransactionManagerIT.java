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
package org.neo4j.fabric;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class TransactionManagerIT {
    @Inject
    private DatabaseManagementService databaseManagementService;

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void failToStartFabricTransactionAfterShutdown() {
        var transactionManager = database.getDependencyResolver().resolveDependency(TransactionManager.class);
        var invocationWitness = new AtomicBoolean(false);

        databaseManagementService.registerDatabaseEventListener(new DatabaseEventListenerAdapter() {
            @Override
            public void databaseShutdown(DatabaseEventContext eventContext) {
                assertThrows(DatabaseShutdownException.class, () -> transactionManager.begin(null, null));
                invocationWitness.set(true);
            }
        });
        databaseManagementService.shutdown();

        assertTrue(invocationWitness.get());
    }
}
