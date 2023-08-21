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

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class DynamicTransactionReadOnlyIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private Config config;

    @Test
    void byDefaultDatabaseIsWritable() {
        assertDoesNotThrow(() -> {
            try (var tx = database.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
    }

    @Test
    void reactToDatabaseReadOnlyMode() {
        config.set(GraphDatabaseSettings.read_only_databases, Set.of(database.databaseName()));
        try {
            try (var tx = database.beginTx()) {
                assertThrows(WriteOperationsNotAllowedException.class, tx::createNode);
            }

            config.set(GraphDatabaseSettings.read_only_databases, emptySet());

            assertDoesNotThrow(() -> {
                try (var tx = database.beginTx()) {
                    tx.createNode();
                    tx.commit();
                }
            });
        } finally {
            config.set(GraphDatabaseSettings.read_only_databases, emptySet());
        }
    }
}
