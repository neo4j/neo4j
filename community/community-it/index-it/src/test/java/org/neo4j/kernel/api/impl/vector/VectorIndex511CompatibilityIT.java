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
package org.neo4j.kernel.api.impl.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class VectorIndex511CompatibilityIT {
    private static final int EXPECTED_INDEX_VECTOR_DIMENSIONS = 1536;
    private static final String ARCHIVE_NAME = "5-11-vector-idx-db.zip";
    private static final String TEXT_IDX_NAME = "VectorIndex";
    private static final String LABEL = "Label";
    private static final String PROPERTY = "property";

    @Inject
    TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseService db;

    @BeforeEach
    void beforeEach() throws Exception {
        Unzip.unzip(getClass(), ARCHIVE_NAME, testDirectory.homePath());

        dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
        db = dbms.database("neo4j");
    }

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @Test
    void existingIndexesWorkWithoutMigration() throws KernelException {
        assertVectorIndexContainsNumberOfValues(10);

        float[] vector = generateVector();

        // add one node old-fashioned way
        try (var tx = db.beginTx()) {
            Node node = tx.createNode(Label.label(LABEL));
            node.setProperty(PROPERTY, vector);

            tx.commit();
        }

        // old index should contain old entries and a new one inserted above now
        try (var tx = db.beginTx()) {
            var vectorQueryResult = tx.execute(
                    "CALL db.index.vector.queryNodes('VectorIndex', 20, $vector) YIELD node AS node, score",
                    Map.of("vector", vector));
            int rowCounter = 0;
            while (vectorQueryResult.hasNext()) {
                rowCounter++;
                var resultMap = vectorQueryResult.next();
                assertThat(resultMap).containsOnlyKeys("node", "score");
            }
            assertEquals(11, rowCounter);
        }
        // dbl check that index entries matching
        assertVectorIndexContainsNumberOfValues(11);
    }

    private float[] generateVector() {
        float[] data = new float[EXPECTED_INDEX_VECTOR_DIMENSIONS];
        var localRandom = ThreadLocalRandom.current();
        for (int i = 0; i < data.length; i++) {
            data[i] = localRandom.nextFloat();
        }
        return data;
    }

    private void assertVectorIndexContainsNumberOfValues(int numberOfValues) throws KernelException {
        try (var tx = db.beginTx()) {
            var kernelTx = ((TransactionImpl) tx).kernelTransaction();

            IndexDescriptor index = kernelTx.schemaRead().indexGetForName(TEXT_IDX_NAME);
            IndexReadSession indexSession = kernelTx.dataRead().indexReadSession(index);

            try (NodeValueIndexCursor cursor = kernelTx.cursors()
                    .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                kernelTx.dataRead()
                        .nodeIndexSeek(
                                kernelTx.queryContext(),
                                indexSession,
                                cursor,
                                unordered(false),
                                PropertyIndexQuery.allEntries());
                int entryCounter = 0;
                while (cursor.next()) {
                    entryCounter++;
                }

                assertEquals(numberOfValues, entryCounter);
            }
        }
    }
}
