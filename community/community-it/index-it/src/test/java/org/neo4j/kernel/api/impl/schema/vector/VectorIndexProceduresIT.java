/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.MathUtil.ceil;
import static org.neo4j.kernel.api.impl.schema.vector.VectorTestUtils.SUGGESTED_SETTINGS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.builtin.VectorIndexProcedures.Neighbor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

class VectorIndexProceduresIT {

    @ImpermanentDbmsExtension(configurationCallback = "configure")
    @ExtendWith(RandomExtension.class)
    abstract static class VectorIndexProceduresITBase {
        private static final int NUMBER_OF_NODES = 1000;
        private static final int VECTOR_DIMENSIONALITY = 1000;
        private static final Label LABEL = Label.label("Vector");
        private static final String PROPERTY_KEY = "vector";
        private static final String INDEX_NAME = "VectorIndex";

        private final VectorSimilarityFunction similarityFunction;

        VectorIndexProceduresITBase(VectorSimilarityFunction similarityFunction) {
            this.similarityFunction = similarityFunction;
        }

        @Inject
        private GraphDatabaseService db;

        @Inject
        private RandomSupport random;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            builder.setConfig(SUGGESTED_SETTINGS);
        }

        @BeforeEach
        void createData() {
            try (final var tx = db.beginTx()) {
                for (int i = 0; i < NUMBER_OF_NODES; i++) {
                    final var node = tx.createNode(LABEL);
                    node.setProperty(PROPERTY_KEY, randomVector());
                }
                tx.commit();
            }
        }

        @Test
        void testCreateAndQueryIndex() {
            assertThatCode(this::createIndex).doesNotThrowAnyException();

            final var k = 10;
            final var query = randomVector();
            assertThatCode(() -> queryNodesAndCollect(k, query)).doesNotThrowAnyException();
        }

        @Test
        void testQuery() {
            createIndex();

            final var k = 10;
            final var query = randomVector();
            final var approximateNearest = queryNodesAndCollect(k, query);
            final var exactNearest = linearSearch(LABEL, PROPERTY_KEY, query, k);
            final var recall = recall(approximateNearest, exactNearest);

            assertThat(approximateNearest).as("approximate nearest neighbors").hasSize(k);
            assertThat(recall).as("recall").isGreaterThan(0.5);
            // TODO VECTOR: what is appropriate recall here?
            //              should we have a larger NUMBER_OF_NODES
        }

        @Test
        void indexOnlyMatchingDimensions() {
            // Change the dimensionality of some vectors
            final var nonIndexedVectors = new HashSet<String>();
            try (final var tx = db.beginTx();
                    final var nodes = tx.findNodes(LABEL)) {
                while (nodes.hasNext()) {
                    // skip ~80% of the time
                    if (random.nextFloat() < 0.8f) {
                        continue;
                    }

                    final var node = nodes.next();
                    node.setProperty(PROPERTY_KEY, randomVector(VECTOR_DIMENSIONALITY - 1));
                    nonIndexedVectors.add(node.getElementId());
                }

                tx.commit();
            }

            createIndex();

            // Ask for all nodes in index and check how many we found
            final var query = randomVector();
            final var indexedVectors = new HashSet<String>();
            try (final var tx = db.beginTx();
                    final var results = queryNodes(tx, NUMBER_OF_NODES, query)) {
                results.accept(row -> indexedVectors.add(row.getNode("node").getElementId()));
            }

            assertThat(indexedVectors)
                    .as("vectors of different dimensions should not be indexed")
                    .doesNotContainAnyElementsOf(nonIndexedVectors);
        }

        @Test
        void cannotQueryWithWrongDimensions() {
            createIndex();
            final var query = randomVector(VECTOR_DIMENSIONALITY - 1);
            try (final var tx = db.beginTx();
                    final var results = queryNodes(tx, 10, query)) {

                assertThatThrownBy(results::resultAsString, "incorrectly dimensioned query should throw")
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll(
                                "Index query vector has",
                                String.valueOf(VECTOR_DIMENSIONALITY - 1),
                                "dimensions",
                                "but indexed vectors have",
                                String.valueOf(VECTOR_DIMENSIONALITY));
            }
        }

        @Test
        void cannotQueryWithInvalidVector() {
            createIndex();
            final var query = randomVector();
            query[random.nextInt(query.length)] = Float.NaN;
            try (final var tx = db.beginTx()) {
                final var results = queryNodes(tx, 10, query);

                assertThatThrownBy(results::resultAsString, "non-finite query vector should throw")
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("Index query vector must contain finite values", "Provided");
            }
        }

        @Test
        void cannotReturnMoreThanMax() {
            createIndex();

            final var k = ceil(NUMBER_OF_NODES * 12, 10); // ~1.2x
            final var query = randomVector();
            final var approximateNearest = queryNodesAndCollect(k, query);

            assertThat(approximateNearest)
                    .as("approximate nearest neighbors")
                    .hasSizeLessThanOrEqualTo(k)
                    .hasSize(NUMBER_OF_NODES);
        }

        @Test
        void testLinearSearch() {
            // testing test code
            final var k = 10;
            final var query = randomVector();
            final var nearest = linearSearch(LABEL, PROPERTY_KEY, query, k);
            assertThat(nearest).hasSize(k).isSorted();
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

        private List<Neighbor> queryNodesAndCollect(int k, float[] query) {
            try (final var tx = db.beginTx();
                    final var results = queryNodes(tx, k, query)) {
                return collectNeighbors(results);
            }
        }

        private static Result queryNodes(Transaction tx, int k, float[] query) {
            return tx.execute(
                    """
                    CALL db.index.vector.queryNodes($name, $k, $query) YIELD node, score
                    RETURN score, node ORDER BY score DESC
                    """,
                    Map.of(
                            "name", INDEX_NAME,
                            "k", k,
                            "query", query));
        }

        private static List<Neighbor> collectNeighbors(Result results) {
            final var neighbors = new ArrayList<Neighbor>();
            results.accept(row -> neighbors.add(
                    new Neighbor(row.getNode("node"), row.getNumber("score").doubleValue())));
            return neighbors;
        }

        private List<Neighbor> linearSearch(Label label, String propertyKey, float[] query, int k) {
            final var results = new ArrayList<Neighbor>();
            try (final var tx = db.beginTx()) {
                final var nodes = tx.findNodes(label);
                while (nodes.hasNext()) {
                    final var node = nodes.next();
                    final var value = node.getProperty(propertyKey);

                    if (value instanceof final float[] vector) {
                        final var score = similarityFunction.compare(query, vector);
                        final int rank = getRankInCurrentResults(results, score);
                        includeInNearestK(results, k, rank, node, score);
                    }
                }
            }
            return results;
        }

        private static void includeInNearestK(List<Neighbor> results, int k, int insertAt, Node node, float score) {
            if (insertAt <= k) {
                results.add(insertAt, new Neighbor(node, score));
                if (results.size() > k) {
                    results.remove(results.size() - 1);
                }
            }
        }

        private static int getRankInCurrentResults(List<Neighbor> current, float score) {
            for (int i = current.size(); i > 0; i--) {
                if (current.get(i - 1).score() > score) {
                    return i;
                }
            }
            return 0;
        }

        // Proportion of true K nearest neighbors that were found by approximate algorithm
        private static double recall(List<Neighbor> approximate, List<Neighbor> exact) {
            long trueNearestFound = exact.stream().filter(approximate::contains).count();
            return trueNearestFound / (double) exact.size();
        }

        private float[] randomVector() {
            return randomVector(VECTOR_DIMENSIONALITY);
        }

        // Draws a random vector from a unit hypercube centered on the origin
        private float[] randomVector(int dimensions) {
            final var vector = new float[dimensions];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = random.nextFloat() - 0.5f;
            }
            return vector;
        }
    }

    @Nested
    class Euclidean extends VectorIndexProceduresITBase {
        Euclidean() {
            super(VectorSimilarityFunction.EUCLIDEAN);
        }
    }

    @Nested
    class Cosine extends VectorIndexProceduresITBase {
        Cosine() {
            super(VectorSimilarityFunction.COSINE);
        }
    }
}
