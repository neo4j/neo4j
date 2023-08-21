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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class JavaValueCompatibilityTest {
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() {
        managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    void collectionsInCollectionsLookAlright() {
        try (Transaction transaction = db.beginTx()) {
            Result result = transaction.execute("CREATE (n:TheNode) RETURN [[ [1,2],[3,4] ],[[5,6]]] as x");
            Map<String, Object> next = result.next();
            @SuppressWarnings("unchecked") // We know it's a collection.
            List<List<Object>> x = (List<List<Object>>) next.get("x");
            Iterable objects = x.get(0);

            assertThat(objects).isInstanceOf(Iterable.class);
            transaction.commit();
        }
    }
}
