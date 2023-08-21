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
package org.neo4j.cypher.internal.javacompat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class CypherUpdateMapTest {
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup() {
        managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void cleanup() {
        managementService.shutdown();
    }

    @Test
    void updateNodeByMapParameter() {
        try (Transaction transaction = db.beginTx()) {
            transaction
                    .execute(
                            "CREATE (n:Reference) SET n = $data RETURN n",
                            map("data", map("key1", "value1", "key2", 1234)))
                    .close();
            transaction.commit();
        }

        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeById(0);
            assertThat(node.getProperty("key1")).isEqualTo("value1");
            assertThat(node.getProperty("key2")).isEqualTo(1234);
        }

        try (Transaction transaction = db.beginTx()) {
            transaction
                    .execute("MATCH (n:Reference) SET n = $data RETURN n", map("data", map("key1", null, "key3", 5678)))
                    .close();
            transaction.commit();
        }

        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeById(0);
            assertThat(node.hasProperty("key1")).isFalse();
            assertThat(node.hasProperty("key2")).isFalse();
            assertThat(node.getProperty("key3")).isEqualTo(5678);
        }
    }
}
