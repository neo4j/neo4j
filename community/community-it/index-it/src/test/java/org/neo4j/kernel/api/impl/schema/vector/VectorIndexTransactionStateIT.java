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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class VectorIndexTransactionStateIT {
    protected static final int VECTOR_DIMENSIONALITY = 1;
    private static final Label LABEL = Label.label("Vector");
    private static final String PROPERTY_KEY = "vector";
    private static final String INDEX_NAME = "VectorIndex";
    private static final VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

    @Inject
    protected GraphDatabaseService db;

    @Test
    void shouldNotIncludeTransactionState() {
        // TODO VECTOR: include transaction state!
        // We currently ignore transaction state altogether; this is just a sanity check.

        createIndex();

        try (final var tx = db.beginTx()) {
            final var node = tx.createNode(LABEL);
            node.setProperty(PROPERTY_KEY, new float[] {1.0f});

            // Querying index here should not return any nodes
            try (final var result = queryNodes(tx, 10)) {
                assertThat(result.hasNext()).isFalse();
            }

            tx.commit();
        }

        try (final var tx = db.beginTx()) {
            // This following transaction should return the written node
            final var result = queryNodes(tx, 10);
            final long results = Iterators.count(result.stream().iterator());
            assertThat(results).isEqualTo(1);
        }
    }

    private void createIndex() {
        db.executeTransactionally(
                "CALL db.index.vector.createNodeIndex($name, $label, $propertyKey, $dimensions, $similarity)",
                Map.of(
                        "name", INDEX_NAME,
                        "label", LABEL.name(),
                        "propertyKey", PROPERTY_KEY,
                        "dimensions", VECTOR_DIMENSIONALITY,
                        "similarity", similarityFunction.name().toLowerCase(Locale.ROOT)));
    }

    private Result queryNodes(Transaction tx, int k) {
        final var queryVector = new float[] {0.0f};
        return tx.execute(
                "CALL db.index.vector.queryNodes($name, $k, $query)",
                Map.of(
                        "name", INDEX_NAME,
                        "k", k,
                        "query", queryVector));
    }
}
