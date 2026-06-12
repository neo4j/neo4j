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
package org.neo4j.kernel.impl.index.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.index.vector.VectorSSEntityFilterTest.Vectors.entityFilter;
import static org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.field;
import static org.neo4j.test.extension.SkipOnSpd.Note.temporary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.values.storable.RandomValuesUtils;
import org.neo4j.values.storable.Values;

@SkipOnSpd(notes = temporary, reason = "Entity filtering not supported via CYPHER")
@RandomSupportExtension
class VectorSSEntityFilterTest extends VectorSSFTestBase {
    private static final int NUM_ENTITIES = 1000;
    private static final int ITERATIONS = 100;

    @Inject
    private RandomSupport random;

    @BeforeEach
    void setup() {
        random.withConfiguration(RandomValuesUtils.selectStorageEngineDependentConfiguration(db))
                .reset();
    }

    @Test
    void filterNodes() throws Exception {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME);
        long alice = createTestNode(Map.of("name", "Alice", EMBEDDING_NAME, EMBEDDINGS.get(0)));
        long bob = createTestNode(Map.of("name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        long carol = createTestNode(Map.of("name", "Carol", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        assertThat(queryNodeIndex(entityFilter())).isEmpty();
        assertThat(queryNodeIndex(entityFilter(alice))).singleElement().has(field("name", "Alice"));
        assertThat(queryNodeIndex(entityFilter(bob))).singleElement().has(field("name", "Bob"));
        assertThat(queryNodeIndex(entityFilter(carol))).singleElement().has(field("name", "Carol"));
        assertThat(queryNodeIndex(entityFilter(alice, bob)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Bob");
        assertThat(queryNodeIndex(entityFilter(alice, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Carol");
        assertThat(queryNodeIndex(entityFilter(bob, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Bob", "Carol");
        assertThat(queryNodeIndex(entityFilter(alice, bob, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Bob", "Carol");
    }

    @Test
    void filterNodesFuzzTest() throws Exception {

        Vectors vectors = new Vectors(SIMILARITY_FUNCTION);

        int dim = random.nextInt(128, 4096);
        createNodeVectorIndex(VECTOR_INDEX_NAME, dim, EMBEDDING_NAME);

        List<Long> allNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                allNodes.add(createTestNode(tx, Map.of(EMBEDDING_NAME, vectors.randomVector(dim))));
            }
            tx.commit();
        }
        // sanity check
        assertThat(vectors.randomNodeQuery(dim)).isEmpty();

        for (int i = 0; i < ITERATIONS; i++) {
            Set<Long> subset = randomSubset(allNodes);
            assertThat(vectors.randomNodeQuery(dim, from(subset)))
                    .extracting(EXTRACT_ENTITY_ID)
                    .containsExactlyInAnyOrderElementsOf(subset);
        }
    }

    @Test
    void filterRelationships() throws Exception {
        createRelationshipVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME);
        long alice = createTestRelationship(Map.of("name", "Alice", EMBEDDING_NAME, EMBEDDINGS.get(0)));
        long bob = createTestRelationship(Map.of("name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        long carol = createTestRelationship(Map.of("name", "Carol", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        assertThat(queryRelationshipIndex(entityFilter())).isEmpty();
        assertThat(queryRelationshipIndex(entityFilter(alice))).singleElement().has(field("name", "Alice"));
        assertThat(queryRelationshipIndex(entityFilter(bob))).singleElement().has(field("name", "Bob"));
        assertThat(queryRelationshipIndex(entityFilter(carol))).singleElement().has(field("name", "Carol"));
        assertThat(queryRelationshipIndex(entityFilter(alice, bob)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Bob");
        assertThat(queryRelationshipIndex(entityFilter(alice, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Carol");
        assertThat(queryRelationshipIndex(entityFilter(bob, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Bob", "Carol");
        assertThat(queryRelationshipIndex(entityFilter(alice, bob, carol)))
                .extracting(EXTRACT_NAME)
                .containsExactlyInAnyOrder("Alice", "Bob", "Carol");
    }

    @Test
    void filterRelationshipsFuzzTest() throws Exception {

        Vectors vectors = new Vectors(SIMILARITY_FUNCTION);

        int dim = random.nextInt(128, 4096);
        createRelationshipVectorIndex(VECTOR_INDEX_NAME, dim, EMBEDDING_NAME);

        List<Long> allRels = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                allRels.add(createTestRelationship(tx, Map.of(EMBEDDING_NAME, vectors.randomVector(dim))));
            }
            tx.commit();
        }
        // sanity check
        assertThat(vectors.randomRelationshipQuery(dim)).isEmpty();

        for (int i = 0; i < ITERATIONS; i++) {
            Set<Long> subset = randomSubset(allRels);
            assertThat(vectors.randomRelationshipQuery(dim, from(subset)))
                    .extracting(EXTRACT_ENTITY_ID)
                    .containsExactlyInAnyOrderElementsOf(subset);
        }
    }

    private Set<Long> randomSubset(List<Long> values) {
        int n = random.nextInt(values.size());
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < n; i++) {
            set.add(random.among(values));
        }
        return set;
    }

    private static long[] from(Set<Long> ids) {
        return ids.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Encapsulate vector and query generation relative to a similarity function
     */
    class Vectors {

        private final VectorSimilarityFunction vectorSimilarityFunction;

        Vectors(VectorSimilarityFunction vectorSimilarityFunction) {
            this.vectorSimilarityFunction = vectorSimilarityFunction;
        }

        private List<VectorSSFQueryResult> randomNodeQuery(int dim, long... entities) throws Exception {
            return queryNodeIndex(
                    VECTOR_INDEX_NAME,
                    PropertyIndexQuery.nearestNeighbors(Integer.MAX_VALUE, randomQueryVector(dim)),
                    entityFilter(entities));
        }

        private List<VectorSSFQueryResult> randomRelationshipQuery(int dim, long... entities) throws Exception {
            return queryRelationshipIndex(
                    VECTOR_INDEX_NAME,
                    PropertyIndexQuery.nearestNeighbors(Integer.MAX_VALUE, randomQueryVector(dim)),
                    entityFilter(entities));
        }

        static EntityFilterPredicate entityFilter(long... entityIds) {
            return PropertyIndexQuery.entityFilter(LongSets.mutable.of(entityIds));
        }

        private float[] randomVector(int dim) {
            float[] vector = random.randomValues().nextFloatArrayRaw(dim, dim);

            // ensure at least one (random) element is not at exact origin, so vector is not all zeros
            int index = random.nextInt(dim);
            if (vector[index] == 0.0f) {
                vector[index] = random.nextBoolean() ? Math.nextUp(vector[index]) : Math.nextDown(vector[index]);
            }

            return vector;
        }

        private float[] randomQueryVector(int dim) {
            return vectorSimilarityFunction.toValidVector(Values.floatArray(randomVector(dim)));
        }
    }
}
