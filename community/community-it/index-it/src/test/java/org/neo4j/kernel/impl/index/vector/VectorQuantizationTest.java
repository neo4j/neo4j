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
import static org.assertj.core.api.Assertions.within;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.RequireAlignedFormat;

// doubles are currently not supported via the block serialization, IND-409
@RequireAlignedFormat
public class VectorQuantizationTest extends VectorSSFTestBase {

    public static final int DIMENSION = 8;

    @Override
    @ExtensionCallback
    protected void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        KernelVersion.VERSION_VECTOR_BINARY_QUANTIZATION.version())
                .setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.fromKernelVersion(KernelVersion.VERSION_VECTOR_BINARY_QUANTIZATION)
                                .getVersion());
    }

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

        float[] queryVector = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f};
        List<VectorSSFQueryResult> allResults = queryNodeIndex(queryVector, 4);

        Object[] expected =
                switch (withRescoring ? VectorQuantizationType.NONE : quantizationType) {
                    case NONE -> new Object[] {4L, 1.146f, 3L, 0.990f, 2L, 0.872f, 1L, 0.777f};
                    case SCALAR -> new Object[] {4L, 1.0f, 3L, 0.990f, 2L, 0.872f, 1L, 0.777f};
                    case BINARY -> new Object[] {4L, 1.0f, 3L, 1.0f, 2L, 0.817f, 1L, 0.754f};
                };

        assertThat(allResults)
                .flatExtracting(EXTRACT_ID, VectorSSFQueryResult::score)
                .usingElementComparator(new CloseToComparator(within(0.001f)))
                .containsExactly(expected);
    }

    record CloseToComparator(Offset<Float> offset) implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Float lhs && o2 instanceof Number rhs) {
                if (Float.compare(Math.abs(lhs - rhs.floatValue()), this.offset.value) <= 0) {
                    return 0;
                }
                return Float.compare(lhs, rhs.floatValue());
            }
            if (o1 instanceof Number lhs && o2 instanceof Number rhs) {
                return Integer.compare(lhs.intValue(), rhs.intValue());
            }
            return -1;
        }
    }
}
