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
package org.neo4j.kernel.impl.api.constraints;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ConstraintCreationIT {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private IndexProviderMap indexProviderMap;

    private static final Label LABEL = Label.label("label1");
    private long indexId;
    private long nbrIndexesOnStart;

    @BeforeEach
    void setUp() {
        try (Transaction tx = db.beginTx()) {
            nbrIndexesOnStart = Iterables.count(tx.schema().getIndexes());
            // The id the index belonging to the constraint should get
            indexId = nbrIndexesOnStart + 1;
        }
    }

    @Test
    void shouldNotLeaveNativeIndexFilesHangingAroundIfConstraintCreationFails() {
        // given
        attemptAndFailConstraintCreation();

        // then
        Path indexDir =
                indexProviderMap.getDefaultProvider().directoryStructure().directoryForIndex(indexId);

        assertFalse(Files.exists(indexDir));
    }

    private void attemptAndFailConstraintCreation() {
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 2; i++) {
                Node node1 = tx.createNode(LABEL);
                node1.setProperty("prop", true);
            }

            tx.commit();
        }

        // when
        assertThrows(ConstraintViolationException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.schema().constraintFor(LABEL).assertPropertyIsUnique("prop").create();
                tx.commit();
            }
        });

        // then
        try (Transaction tx = db.beginTx()) {
            assertEquals(nbrIndexesOnStart, Iterables.count(tx.schema().getIndexes()));
        }
    }
}
