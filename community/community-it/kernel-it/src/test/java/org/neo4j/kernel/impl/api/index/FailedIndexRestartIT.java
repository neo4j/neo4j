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
package org.neo4j.kernel.impl.api.index;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class FailedIndexRestartIT {
    private static final String ROBOT = "Robot";
    private static final String GENDER = "gender";

    @Inject
    private GraphDatabaseService database;

    @Inject
    private DbmsController dbmsController;

    @Test
    void failedIndexUpdatesAfterRestart() {
        Label robot = Label.label(ROBOT);
        String megaProperty = randomAlphanumeric((int) mebiBytes(16));
        createNodeWithProperty(database, robot, megaProperty);

        try (Transaction tx = database.beginTx()) {
            tx.schema().indexFor(robot).on(GENDER).create();
            tx.commit();
        }

        assertThatThrownBy(() -> awaitIndexesOnline(database))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll("IllegalArgumentException", "Property value is too large to index");

        // can add more nodes that do not satisfy failed index
        createNodeWithProperty(database, robot, megaProperty);
        try (Transaction transaction = database.beginTx()) {
            assertThat(count(transaction.findNodes(robot))).isEqualTo(2);
        }

        dbmsController.restartDbms();

        // can add more nodes that do not satisfy failed index after db and index restart
        createNodeWithProperty(database, robot, megaProperty);
        try (Transaction transaction = database.beginTx()) {
            assertThat(count(transaction.findNodes(robot))).isEqualTo(3);
        }
    }

    private static void createNodeWithProperty(GraphDatabaseService db, Label label, String propertyValue) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(GENDER, propertyValue);
            tx.commit();
        }
    }

    private static void awaitIndexesOnline(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(3, TimeUnit.MINUTES);
        }
    }
}
