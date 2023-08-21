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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class PropertyKeyTest {
    @Inject
    private GraphDatabaseService db;

    @Inject
    DbmsController dbmsController;

    @Test
    void lazyLoadWithinWriteTransaction() throws IOException {
        int count = 1000;
        String nodeId;
        try (var tx = db.beginTx()) {
            var node = tx.createNode();
            mapWithManyProperties(count).forEach(node::setProperty);
            nodeId = node.getElementId();
            tx.commit();
        }

        dbmsController.restartDbms();
        assertThat(db.isAvailable(TimeUnit.MINUTES.toMillis(5))).isTrue();

        // When
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            var node = tx.getNodeByElementId(nodeId);

            // Then
            assertThat(node.getPropertyKeys()).hasSize(count);
            tx.commit();
        }
    }

    private static Map<String, Object> mapWithManyProperties(int count) {
        Map<String, Object> properties = new HashMap<>();
        for (int i = 0; i < count; i++) {
            properties.put("key:" + i, "value");
        }
        return properties;
    }
}
