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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.MathUtil.ceil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils;
import org.neo4j.procedure.builtin.VectorIndexProcedures.Neighbor;
import org.neo4j.procedure.builtin.VectorIndexProcedures.NodeRecord;
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
        private static final int MAX_PARTITION_SIZE = 400;
        protected static final int VECTOR_DIMENSIONALITY = 1000;
        private static final Label LABEL = Label.label("Vector");
        private static final String PROPERTY_KEY = "vector";
        private static final String INDEX_NAME = "VectorIndex";

        private final VectorSimilarityFunction similarityFunction;

        VectorIndexProceduresITBase(VectorSimilarityFunction similarityFunction) {
            this.similarityFunction = similarityFunction;
        }

        @Inject
        protected GraphDatabaseService db;

        @Inject
        protected RandomSupport random;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            builder.setConfig(GraphDatabaseInternalSettings.lucene_max_partition_size, MAX_PARTITION_SIZE);
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
        void testRecall() {
            createIndex();

            final var k = 10;
            final var query = randomVector();
            final var approximateNearest = queryNodesAndCollect(k, query);
            final var exactNearest = linearSearch(LABEL, PROPERTY_KEY, query, k);
            final var recall = recall(approximateNearest, exactNearest);

            assertThat(approximateNearest)
                    .as("approximate nearest neighbors")
                    .hasSize(k)
                    .isSorted();

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
        void cannotQueryWithNonFiniteVector() {
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

        private static Iterable<List<Double>> canSetValidVector() {
            // Euclidean is the bare minimum invariant
            return VectorTestUtils.EUCLIDEAN_VALID_VECTORS_FROM_DOUBLE_LIST;
        }

        @ParameterizedTest
        @MethodSource
        void canSetValidVector(List<Double> candidate) {
            final long id;
            try (final var tx = db.beginTx()) {
                final var node = tx.createNode(LABEL);
                id = node.getId();
                try (final var result = setVectorProperty(tx, node, candidate)) {
                    assertThatCode(() -> collectNodes(result)).doesNotThrowAnyException();
                }
                tx.commit();
            }

            try (final var tx = db.beginTx()) {
                final var node = tx.getNodeById(id);
                assertThat(node.getPropertyKeys()).contains(PROPERTY_KEY);
                final var vector = node.getProperty(PROPERTY_KEY);
                assertThat(vector)
                        .asInstanceOf(InstanceOfAssertFactories.FLOAT_ARRAY)
                        .containsExactly(Lists.immutable
                                .ofAll(candidate)
                                .collectFloat(Double::floatValue)
                                .toArray());
            }
        }

        private static Iterable<List<Double>> cannotSetInvalidVector() {
            // Euclidean is the bare minimum invariant
            return VectorTestUtils.EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST;
        }

        @ParameterizedTest
        @MethodSource
        void cannotSetInvalidVector(List<Double> candidate) {
            try (final var tx = db.beginTx()) {
                final var node = tx.createNode(LABEL);
                final var rootAssert = assertThatThrownBy(
                                () -> setVectorProperty(tx, node, candidate), "invalid vectors should throw")
                        .rootCause();

                if (candidate != null) {
                    rootAssert
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContainingAll("Index query vector must contain finite values", "Provided");
                } else {
                    rootAssert
                            .isInstanceOf(NullPointerException.class)
                            .hasMessageContainingAll("vector", "must not be null");
                }
            }
        }

        @Test
        void cannotReturnMoreThanMax() {
            createIndex();

            final var k = ceil(NUMBER_OF_NODES * 12, 10); // ~1.2x
            final var query = randomVector();
            final var approximateNearest = queryNodesAndCollect(k, query);

            assertThat(approximateNearest).as("approximate nearest neighbors").hasSizeLessThanOrEqualTo(k);

            // cannot assertThat(approximateNearest).hasSize(NUMBER_OF_NODES):
            // org.apache.lucene.codecs.KnnVectorsReader::search
            //
            // The search is allowed to be approximate, meaning the results are not guaranteed to be the true k
            // closest neighbors. For large values of k (for example when k is close to the total number of
            // documents), the search may also retrieve fewer than k documents.
        }

        @Test
        void testLinearSearch() {
            // testing test code
            final var k = 10;
            final var query = randomVector();
            final var nearest = linearSearch(LABEL, PROPERTY_KEY, query, k);
            assertThat(nearest).hasSize(k).isSorted();
        }

        record ParameterVariation(String parameter, Object value, Optional<String> expectedError) {
            void assertValidation(Map<String, Object> defaults, Consumer<Map<String, Object>> action) {
                final var parameters = new HashMap<>(defaults);
                parameters.put(parameter, value);

                final var assertion = assertThatCode(() -> action.accept(parameters));
                if (expectedError.isPresent()) {
                    assertion.hasMessageContaining(expectedError.get());
                } else {
                    assertion.doesNotThrowAnyException();
                }
            }
        }

        protected static Stream<ParameterVariation> shouldValidateCreationParameters() {
            return Stream.of(
                    new ParameterVariation("name", null, Optional.of("'indexName' must not be null")),
                    new ParameterVariation("label", null, Optional.of("'label' must not be null")),
                    new ParameterVariation("propertyKey", null, Optional.of("'propertyKey' must not be null")),
                    new ParameterVariation("vectorDimension", null, Optional.of("'vectorDimension' must not be null")),
                    new ParameterVariation(
                            "vectorDimension",
                            0,
                            Optional.of("'vectorDimension' must be between %d and %d inclusively"
                                    .formatted(1, VectorUtils.MAX_DIMENSIONS))),
                    new ParameterVariation("vectorDimension", 1, Optional.empty()), // OK
                    new ParameterVariation("vectorDimension", VectorUtils.MAX_DIMENSIONS, Optional.empty()), // OK
                    new ParameterVariation(
                            "vectorDimension",
                            VectorUtils.MAX_DIMENSIONS + 1,
                            Optional.of("'vectorDimension' must be between %d and %d inclusively"
                                    .formatted(1, VectorUtils.MAX_DIMENSIONS))),
                    new ParameterVariation(
                            "vectorSimilarityFunction",
                            null,
                            Optional.of("'vectorSimilarityFunction' must not be null")),
                    new ParameterVariation(
                            "vectorSimilarityFunction",
                            "nonsense",
                            Optional.of("'nonsense' is an unsupported vector similarity function")));
        }

        @ParameterizedTest
        @MethodSource
        protected void shouldValidateCreationParameters(ParameterVariation variation) {
            variation.assertValidation(defaultCreationParameters(), this::createIndex);
        }

        protected static Stream<ParameterVariation> shouldValidateQueryParameters() {
            return Stream.of(
                    new ParameterVariation("name", null, Optional.of("'indexName' must not be null")),
                    new ParameterVariation(
                            "numberOfNearestNeighbours",
                            null,
                            Optional.of("'numberOfNearestNeighbours' must not be null")),
                    new ParameterVariation(
                            "numberOfNearestNeighbours",
                            0,
                            Optional.of("'numberOfNearestNeighbours' must be positive")),
                    new ParameterVariation("numberOfNearestNeighbours", 1, Optional.empty()), // OK
                    new ParameterVariation("query", null, Optional.of("'query' must not be null")));
        }

        @ParameterizedTest
        @MethodSource
        protected void shouldValidateQueryParameters(ParameterVariation variation) {
            createIndex(defaultCreationParameters());
            variation.assertValidation(
                    defaultQueryParameters(10, randomVector(VECTOR_DIMENSIONALITY)), this::queryNodesAndCollect);
        }

        private void createIndex(Map<String, Object> parameters) {
            db.executeTransactionally(
                    "CALL db.index.vector.createNodeIndex($name, $label, $propertyKey, $vectorDimension, $vectorSimilarityFunction)",
                    parameters);
        }

        protected void createIndex() {
            createIndex(defaultCreationParameters());
        }

        private Map<String, Object> defaultCreationParameters() {
            return Map.of(
                    "name", INDEX_NAME,
                    "label", LABEL.name(),
                    "propertyKey", PROPERTY_KEY,
                    "vectorDimension", VECTOR_DIMENSIONALITY,
                    "vectorSimilarityFunction", similarityFunction.name().toLowerCase(Locale.ROOT));
        }

        private List<Neighbor> queryNodesAndCollect(int k, float[] query) {
            return queryNodesAndCollect(defaultQueryParameters(k, query));
        }

        private List<Neighbor> queryNodesAndCollect(Map<String, Object> parameters) {
            try (final var tx = db.beginTx();
                    final var results = queryNodes(tx, parameters)) {
                return collectNeighbors(results);
            }
        }

        protected static Result queryNodes(Transaction tx, int k, float[] query) {
            return queryNodes(tx, defaultQueryParameters(k, query));
        }

        private static Result queryNodes(Transaction tx, Map<String, Object> parameters) {
            return tx.execute(
                    """
                    CALL db.index.vector.queryNodes($name, $numberOfNearestNeighbours, $query) YIELD node, score
                    RETURN score, node ORDER BY score DESC
                    """,
                    parameters);
        }

        private static Map<String, Object> defaultQueryParameters(Integer k, float[] query) {
            return Map.of(
                    "name", INDEX_NAME,
                    "numberOfNearestNeighbours", k,
                    "query", query);
        }

        private static Result setVectorProperty(Transaction tx, Node node, List<Double> vector) {
            return tx.execute(
                    "CALL db.create.setVectorProperty($node, $propKey, $vector)",
                    Maps.mutable.of("node", node, "propKey", PROPERTY_KEY, "vector", vector));
        }

        private static List<Neighbor> collectNeighbors(Result results) {
            final var neighbors = new ArrayList<Neighbor>();
            results.accept(row -> neighbors.add(
                    new Neighbor(row.getNode("node"), row.getNumber("score").doubleValue())));
            return neighbors;
        }

        private static List<NodeRecord> collectNodes(Result results) {
            final var nodes = new ArrayList<NodeRecord>();
            results.accept(row -> nodes.add(new NodeRecord(row.getNode("node"))));
            return nodes;
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

        protected float[] randomVector() {
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

        @Test
        void cannotQueryWithZerol2NormVector() {
            createIndex();
            final var query = new float[VECTOR_DIMENSIONALITY];
            Arrays.fill(query, 0.f);
            try (final var tx = db.beginTx()) {
                final var results = queryNodes(tx, 10, query);

                assertThatThrownBy(results::resultAsString, "zero l2-norm query vector should throw")
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("have positive and finite l2-norm", "Provided");
            }
        }

        @Test
        void cannotQueryWithNonFinitel2NormVector() {
            createIndex();
            final var query = randomVector();
            query[random.nextInt(query.length)] = Float.MAX_VALUE;
            try (final var tx = db.beginTx()) {
                final var results = queryNodes(tx, 10, query);

                assertThatThrownBy(results::resultAsString, "non-finite l2-norm query vector should throw")
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("have positive and finite l2-norm", "Provided");
            }
        }
    }
}
