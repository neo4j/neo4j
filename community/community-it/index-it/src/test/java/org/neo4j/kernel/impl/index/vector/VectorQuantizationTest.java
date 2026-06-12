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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import org.assertj.core.util.FloatComparator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.SkipOnSpd.Note;
import org.neo4j.values.storable.Values;

public class VectorQuantizationTest extends VectorSSFTestBase {
    public static final int DIMENSION = 8;

    @SkipOnSpd(
            notes = Note.irrelevant,
            reason = "Quantization depends on the vectors within the segment, "
                    + "SPD distributes the nodes over the shards.")
    @ParameterizedTest
    @EnumSource(VectorQuantizationType.class)
    void testQuantizationImpact(VectorQuantizationType quantizationType) throws Exception {
        createNodeVectorIndex(
                VECTOR_INDEX_NAME,
                DIMENSION,
                indexConfig ->
                        indexConfig.withQuantizationType(quantizationType).withDefaultSearchExpansionFactor(1.0),
                EMBEDDING_NAME);

        runQuantizationTest(quantizationType, false);
    }

    @ParameterizedTest
    @EnumSource(VectorQuantizationType.class)
    void testQuantizationRescoring(VectorQuantizationType quantizationType) throws Exception {
        createNodeVectorIndex(
                VECTOR_INDEX_NAME,
                DIMENSION,
                indexConfig -> indexConfig.withQuantizationType(quantizationType),
                EMBEDDING_NAME);

        runQuantizationTest(quantizationType, true);
    }

    void runQuantizationTest(VectorQuantizationType quantizationType, boolean withRescoring) throws Exception {
        float[] vector1 = {0.1f, 0.2f, 0.3f, 12.0f, 0.5f, 0.6f, 0.7f, 0.8f};
        float[] vector2 = {0.1f, 0.2f, 4.0f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f};
        float[] vector3 = {0.1f, 2.0f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f};
        float[] vector4 = {0.8f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f};

        try (Transaction tx = db.beginTx()) {
            createTestNode(tx, Map.of("id", 1, EMBEDDING_NAME, vector1));
            createTestNode(tx, Map.of("id", 2, EMBEDDING_NAME, vector2));
            createTestNode(tx, Map.of("id", 3, EMBEDDING_NAME, vector3));
            createTestNode(tx, Map.of("id", 4, EMBEDDING_NAME, vector4));
            tx.commit();
        }

        float[] queryVector = SIMILARITY_FUNCTION.toValidVector(
                Values.floatArray(new float[] {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f}));
        List<VectorSSFQueryResult> allResults = queryNodeIndex(queryVector, 4);

        SequencedCollection<IdScore> expected = // rescoring uses unquantized vector thus will be same NONE
                switch (withRescoring ? VectorQuantizationType.NONE : quantizationType) {
                    case NONE ->
                        List.of(
                                new IdScore(4L, 0.952f),
                                new IdScore(3L, 0.843f),
                                new IdScore(2L, 0.760f),
                                new IdScore(1L, 0.694f));
                    case SCALAR ->
                        List.of(
                                new IdScore(4L, 0.952f),
                                new IdScore(3L, 0.843f),
                                new IdScore(2L, 0.760f),
                                new IdScore(1L, 0.694f));
                    case BINARY ->
                        List.of(
                                new IdScore(3L, 0.992f),
                                new IdScore(4L, 0.978f),
                                new IdScore(2L, 0.715f),
                                new IdScore(1L, 0.680f));
                };

        assertThat(allResults)
                .map(IdScore::new)
                .usingElementComparator(Comparator.comparingLong(IdScore::id)
                        .thenComparing(IdScore::score, new FloatComparator(0.001f)))
                .containsExactlyElementsOf(expected);
    }

    private record IdScore(long id, float score) {
        IdScore(VectorSSFQueryResult result) {
            this(EXTRACT_ID.apply(result), result.score());
        }
    }
}
