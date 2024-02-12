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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.ALL_ENTRIES;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.BOUNDING_BOX;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.EXACT;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.EXISTS;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.FULLTEXT_SEARCH;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.NEAREST_NEIGHBORS;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.STRING_CONTAINS;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.STRING_PREFIX;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.STRING_SUFFIX;
import static org.neo4j.internal.schema.IndexQuery.IndexQueryType.TOKEN_LOOKUP;
import static org.neo4j.values.storable.ValueCategory.ANYTHING;
import static org.neo4j.values.storable.ValueCategory.BOOLEAN;
import static org.neo4j.values.storable.ValueCategory.BOOLEAN_ARRAY;
import static org.neo4j.values.storable.ValueCategory.GEOMETRY;
import static org.neo4j.values.storable.ValueCategory.GEOMETRY_ARRAY;
import static org.neo4j.values.storable.ValueCategory.NO_CATEGORY;
import static org.neo4j.values.storable.ValueCategory.NUMBER;
import static org.neo4j.values.storable.ValueCategory.NUMBER_ARRAY;
import static org.neo4j.values.storable.ValueCategory.TEMPORAL;
import static org.neo4j.values.storable.ValueCategory.TEMPORAL_ARRAY;
import static org.neo4j.values.storable.ValueCategory.TEXT_ARRAY;
import static org.neo4j.values.storable.ValueCategory.UNKNOWN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexCapability;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.impl.index.schema.PointIndexProvider;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.values.storable.ValueCategory;

class IndexCapabilityTest {
    private static final IndexCapability RANGE = RangeIndexProvider.CAPABILITY;
    private static final IndexCapability POINT = PointIndexProvider.CAPABILITY;
    private static final IndexCapability TEXT = TextIndexProvider.CAPABILITY;
    private static final IndexCapability TRIGRAM = TrigramIndexProvider.CAPABILITY;
    private static final IndexCapability VECTOR_V1 = VectorIndexProvider.capability(
            VectorIndexVersion.V1_0, IndexSettingUtil.defaultConfigForTest(IndexType.VECTOR));
    private static final IndexCapability VECTOR_V2 = VectorIndexProvider.capability(
            VectorIndexVersion.V2_0, IndexSettingUtil.defaultConfigForTest(IndexType.VECTOR));
    private static final IndexCapability TOKEN = TokenIndexProvider.capability(true);
    private static final IndexCapability BLOCK_REL_TOKEN = TokenIndexProvider.capability(false);
    private static final IndexCapability FULLTEXT = new FulltextIndexCapability(false);
    private static final IndexCapability[] ALL = of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2, TOKEN, FULLTEXT);
    private static final IndexCapability[] NONE = of();

    @Test
    void testSupportsOrdering() {
        assertThat(RANGE.supportsOrdering()).isTrue();
        assertThat(POINT.supportsOrdering()).isFalse();
        assertThat(TEXT.supportsOrdering()).isFalse();
        assertThat(TRIGRAM.supportsOrdering()).isFalse();
        assertThat(VECTOR_V1.supportsOrdering()).isFalse();
        assertThat(VECTOR_V2.supportsOrdering()).isFalse();
        assertThat(TOKEN.supportsOrdering()).isTrue();
        assertThat(BLOCK_REL_TOKEN.supportsOrdering()).isFalse();
        assertThat(FULLTEXT.supportsOrdering()).isFalse();
    }

    @Test
    void testSupportsReturningValues() {
        assertThat(RANGE.supportsReturningValues()).isTrue();
        assertThat(POINT.supportsReturningValues()).isTrue();
        assertThat(TEXT.supportsReturningValues()).isFalse();
        assertThat(TRIGRAM.supportsReturningValues()).isFalse();
        assertThat(VECTOR_V1.supportsReturningValues()).isFalse();
        assertThat(VECTOR_V2.supportsReturningValues()).isFalse();
        assertThat(TOKEN.supportsReturningValues()).isTrue();
        assertThat(FULLTEXT.supportsReturningValues()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("supportedValueCategories")
    void testAreValueCategoriesAcceptedRange(IndexCapability capability, ValueCategory[] supportedValueCategory) {
        for (ValueCategory valueCategory : ValueCategory.values()) {
            var expected = Arrays.asList(supportedValueCategory).contains(valueCategory);
            assertThat(capability.areValueCategoriesAccepted(valueCategory)).isEqualTo(expected);
        }
    }

    @ParameterizedTest
    @MethodSource("supportedQueries")
    void testIsQuerySupported(
            IndexQueryType queryType, ValueCategory valueCategory, IndexCapability[] expectedToSupport) {
        List<IndexCapability> expectedNotToSupport = new ArrayList<>(Arrays.asList(ALL));
        for (IndexCapability indexCapability : expectedToSupport) {
            var actual = indexCapability.isQuerySupported(queryType, valueCategory);
            assertThat(actual).as("expect " + indexCapability + " to support").isTrue();
            expectedNotToSupport.remove(indexCapability);
        }
        for (IndexCapability indexCapability : expectedNotToSupport) {
            var actual = indexCapability.isQuerySupported(queryType, valueCategory);
            assertThat(actual)
                    .as("expect " + indexCapability + " to not support")
                    .isFalse();
        }
    }

    @Test
    void testGetCostMultiplier() {
        // EXACT
        assertThat(RANGE.getCostMultiplier(EXACT)).isLessThan(TEXT.getCostMultiplier(EXACT));
        assertThat(RANGE.getCostMultiplier(EXACT)).isLessThan(TRIGRAM.getCostMultiplier(EXACT));
        assertThat(TEXT.getCostMultiplier(EXACT)).isLessThan(TRIGRAM.getCostMultiplier(EXACT));
        // RANGE
        assertThat(RANGE.getCostMultiplier(IndexQueryType.RANGE))
                .isLessThan(TEXT.getCostMultiplier(IndexQueryType.RANGE));
        assertThat(RANGE.getCostMultiplier(IndexQueryType.RANGE))
                .isLessThan(TRIGRAM.getCostMultiplier(IndexQueryType.RANGE));
        assertThat(TEXT.getCostMultiplier(IndexQueryType.RANGE))
                .isLessThan(TRIGRAM.getCostMultiplier(IndexQueryType.RANGE));
        // STRING_PREFIX
        assertThat(RANGE.getCostMultiplier(STRING_PREFIX)).isLessThan(TEXT.getCostMultiplier(STRING_PREFIX));
        assertThat(RANGE.getCostMultiplier(STRING_PREFIX)).isLessThan(TRIGRAM.getCostMultiplier(STRING_PREFIX));
        assertThat(TEXT.getCostMultiplier(STRING_PREFIX)).isLessThan(TRIGRAM.getCostMultiplier(STRING_PREFIX));
        // STRING_SUFFIX
        assertThat(TEXT.getCostMultiplier(STRING_SUFFIX)).isGreaterThan(TRIGRAM.getCostMultiplier(STRING_SUFFIX));
        // STRING_CONTAINS
        assertThat(TEXT.getCostMultiplier(STRING_CONTAINS)).isGreaterThan(TRIGRAM.getCostMultiplier(STRING_CONTAINS));
    }

    public static Stream<Arguments> supportedQueries() {
        return Stream.of(
                // TOKEN_LOOKUP
                Arguments.of(TOKEN_LOOKUP, NUMBER, NONE),
                Arguments.of(TOKEN_LOOKUP, NUMBER_ARRAY, NONE),
                Arguments.of(TOKEN_LOOKUP, ValueCategory.TEXT, NONE),
                Arguments.of(TOKEN_LOOKUP, TEXT_ARRAY, NONE),
                Arguments.of(TOKEN_LOOKUP, GEOMETRY, NONE),
                Arguments.of(TOKEN_LOOKUP, GEOMETRY_ARRAY, NONE),
                Arguments.of(TOKEN_LOOKUP, TEMPORAL, NONE),
                Arguments.of(TOKEN_LOOKUP, TEMPORAL_ARRAY, NONE),
                Arguments.of(TOKEN_LOOKUP, BOOLEAN, NONE),
                Arguments.of(TOKEN_LOOKUP, BOOLEAN_ARRAY, NONE),
                Arguments.of(TOKEN_LOOKUP, UNKNOWN, NONE),
                Arguments.of(TOKEN_LOOKUP, NO_CATEGORY, of(TOKEN)),
                Arguments.of(TOKEN_LOOKUP, ANYTHING, NONE),
                // ALL_ENTRIES :: not supported by TOKEN and FULLTEXT, and ValueCategory is ignored
                Arguments.of(ALL_ENTRIES, NUMBER, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, NUMBER_ARRAY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, ValueCategory.TEXT, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, TEXT_ARRAY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, GEOMETRY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, GEOMETRY_ARRAY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, TEMPORAL, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, TEMPORAL_ARRAY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, BOOLEAN, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, BOOLEAN_ARRAY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, UNKNOWN, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, NO_CATEGORY, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                Arguments.of(ALL_ENTRIES, ANYTHING, of(RANGE, POINT, TEXT, TRIGRAM, VECTOR_V1, VECTOR_V2)),
                // EXISTS
                Arguments.of(EXISTS, NUMBER, of(RANGE)),
                Arguments.of(EXISTS, NUMBER_ARRAY, of(RANGE)),
                Arguments.of(EXISTS, ValueCategory.TEXT, of(RANGE)),
                Arguments.of(EXISTS, TEXT_ARRAY, of(RANGE)),
                Arguments.of(EXISTS, GEOMETRY, of(RANGE)),
                Arguments.of(EXISTS, GEOMETRY_ARRAY, of(RANGE)),
                Arguments.of(EXISTS, TEMPORAL, of(RANGE)),
                Arguments.of(EXISTS, TEMPORAL_ARRAY, of(RANGE)),
                Arguments.of(EXISTS, BOOLEAN, of(RANGE)),
                Arguments.of(EXISTS, BOOLEAN_ARRAY, of(RANGE)),
                Arguments.of(EXISTS, UNKNOWN, of(RANGE)),
                Arguments.of(EXISTS, NO_CATEGORY, of(RANGE)),
                Arguments.of(EXISTS, ANYTHING, of(RANGE)),
                // EXACT
                Arguments.of(EXACT, NUMBER, of(RANGE)),
                Arguments.of(EXACT, NUMBER_ARRAY, of(RANGE)),
                Arguments.of(EXACT, ValueCategory.TEXT, of(RANGE, TEXT, TRIGRAM)),
                Arguments.of(EXACT, TEXT_ARRAY, of(RANGE)),
                Arguments.of(EXACT, GEOMETRY, of(RANGE, POINT)),
                Arguments.of(EXACT, GEOMETRY_ARRAY, of(RANGE)),
                Arguments.of(EXACT, TEMPORAL, of(RANGE)),
                Arguments.of(EXACT, TEMPORAL_ARRAY, of(RANGE)),
                Arguments.of(EXACT, BOOLEAN, of(RANGE)),
                Arguments.of(EXACT, BOOLEAN_ARRAY, of(RANGE)),
                Arguments.of(EXACT, UNKNOWN, of(RANGE)),
                Arguments.of(EXACT, NO_CATEGORY, of(RANGE)),
                Arguments.of(EXACT, ANYTHING, of(RANGE)),
                // RANGE
                Arguments.of(IndexQueryType.RANGE, NUMBER, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, NUMBER_ARRAY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, ValueCategory.TEXT, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, TEXT_ARRAY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, GEOMETRY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, GEOMETRY_ARRAY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, TEMPORAL, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, TEMPORAL_ARRAY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, BOOLEAN, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, BOOLEAN_ARRAY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, UNKNOWN, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, NO_CATEGORY, of(RANGE)),
                Arguments.of(IndexQueryType.RANGE, ANYTHING, of(RANGE)),
                // BOUNDING_BOX
                Arguments.of(BOUNDING_BOX, NUMBER, NONE),
                Arguments.of(BOUNDING_BOX, NUMBER_ARRAY, NONE),
                Arguments.of(BOUNDING_BOX, ValueCategory.TEXT, NONE),
                Arguments.of(BOUNDING_BOX, TEXT_ARRAY, NONE),
                Arguments.of(BOUNDING_BOX, GEOMETRY, of(POINT)),
                Arguments.of(BOUNDING_BOX, GEOMETRY_ARRAY, NONE),
                Arguments.of(BOUNDING_BOX, TEMPORAL, NONE),
                Arguments.of(BOUNDING_BOX, TEMPORAL_ARRAY, NONE),
                Arguments.of(BOUNDING_BOX, BOOLEAN, NONE),
                Arguments.of(BOUNDING_BOX, BOOLEAN_ARRAY, NONE),
                Arguments.of(BOUNDING_BOX, UNKNOWN, NONE),
                Arguments.of(BOUNDING_BOX, NO_CATEGORY, NONE),
                Arguments.of(BOUNDING_BOX, ANYTHING, NONE),
                // STRING_PREFIX
                Arguments.of(STRING_PREFIX, NUMBER, of(RANGE)),
                Arguments.of(STRING_PREFIX, NUMBER_ARRAY, of(RANGE)),
                Arguments.of(STRING_PREFIX, ValueCategory.TEXT, of(RANGE, TEXT, TRIGRAM)),
                Arguments.of(STRING_PREFIX, TEXT_ARRAY, of(RANGE)),
                Arguments.of(STRING_PREFIX, GEOMETRY, of(RANGE)),
                Arguments.of(STRING_PREFIX, GEOMETRY_ARRAY, of(RANGE)),
                Arguments.of(STRING_PREFIX, TEMPORAL, of(RANGE)),
                Arguments.of(STRING_PREFIX, TEMPORAL_ARRAY, of(RANGE)),
                Arguments.of(STRING_PREFIX, BOOLEAN, of(RANGE)),
                Arguments.of(STRING_PREFIX, BOOLEAN_ARRAY, of(RANGE)),
                Arguments.of(STRING_PREFIX, UNKNOWN, of(RANGE)),
                Arguments.of(STRING_PREFIX, NO_CATEGORY, of(RANGE)),
                Arguments.of(STRING_PREFIX, ANYTHING, of(RANGE)),
                // STRING_SUFFIX
                Arguments.of(STRING_SUFFIX, NUMBER, NONE),
                Arguments.of(STRING_SUFFIX, NUMBER_ARRAY, NONE),
                Arguments.of(STRING_SUFFIX, ValueCategory.TEXT, of(TEXT, TRIGRAM)),
                Arguments.of(STRING_SUFFIX, TEXT_ARRAY, NONE),
                Arguments.of(STRING_SUFFIX, GEOMETRY, NONE),
                Arguments.of(STRING_SUFFIX, GEOMETRY_ARRAY, NONE),
                Arguments.of(STRING_SUFFIX, TEMPORAL, NONE),
                Arguments.of(STRING_SUFFIX, TEMPORAL_ARRAY, NONE),
                Arguments.of(STRING_SUFFIX, BOOLEAN, NONE),
                Arguments.of(STRING_SUFFIX, BOOLEAN_ARRAY, NONE),
                Arguments.of(STRING_SUFFIX, UNKNOWN, NONE),
                Arguments.of(STRING_SUFFIX, NO_CATEGORY, NONE),
                Arguments.of(STRING_SUFFIX, ANYTHING, NONE),
                // STRING_CONTAINS
                Arguments.of(STRING_CONTAINS, NUMBER, NONE),
                Arguments.of(STRING_CONTAINS, NUMBER_ARRAY, NONE),
                Arguments.of(STRING_CONTAINS, ValueCategory.TEXT, of(TEXT, TRIGRAM)),
                Arguments.of(STRING_CONTAINS, TEXT_ARRAY, NONE),
                Arguments.of(STRING_CONTAINS, GEOMETRY, NONE),
                Arguments.of(STRING_CONTAINS, GEOMETRY_ARRAY, NONE),
                Arguments.of(STRING_CONTAINS, TEMPORAL, NONE),
                Arguments.of(STRING_CONTAINS, TEMPORAL_ARRAY, NONE),
                Arguments.of(STRING_CONTAINS, BOOLEAN, NONE),
                Arguments.of(STRING_CONTAINS, BOOLEAN_ARRAY, NONE),
                Arguments.of(STRING_CONTAINS, UNKNOWN, NONE),
                Arguments.of(STRING_CONTAINS, NO_CATEGORY, NONE),
                Arguments.of(STRING_CONTAINS, ANYTHING, NONE),
                // FULLTEXT_SEARCH
                Arguments.of(FULLTEXT_SEARCH, NUMBER, NONE),
                Arguments.of(FULLTEXT_SEARCH, NUMBER_ARRAY, NONE),
                Arguments.of(FULLTEXT_SEARCH, ValueCategory.TEXT, of(FULLTEXT)),
                Arguments.of(FULLTEXT_SEARCH, TEXT_ARRAY, NONE),
                Arguments.of(FULLTEXT_SEARCH, GEOMETRY, NONE),
                Arguments.of(FULLTEXT_SEARCH, GEOMETRY_ARRAY, NONE),
                Arguments.of(FULLTEXT_SEARCH, TEMPORAL, NONE),
                Arguments.of(FULLTEXT_SEARCH, TEMPORAL_ARRAY, NONE),
                Arguments.of(FULLTEXT_SEARCH, BOOLEAN, NONE),
                Arguments.of(FULLTEXT_SEARCH, BOOLEAN_ARRAY, NONE),
                Arguments.of(FULLTEXT_SEARCH, UNKNOWN, NONE),
                Arguments.of(FULLTEXT_SEARCH, NO_CATEGORY, NONE),
                Arguments.of(FULLTEXT_SEARCH, ANYTHING, NONE),
                // NEAREST_NEIGHBORS
                Arguments.of(NEAREST_NEIGHBORS, NUMBER, NONE),
                Arguments.of(NEAREST_NEIGHBORS, NUMBER_ARRAY, of(VECTOR_V1, VECTOR_V2)),
                Arguments.of(NEAREST_NEIGHBORS, ValueCategory.TEXT, NONE),
                Arguments.of(NEAREST_NEIGHBORS, TEXT_ARRAY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, GEOMETRY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, GEOMETRY_ARRAY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, TEMPORAL, NONE),
                Arguments.of(NEAREST_NEIGHBORS, TEMPORAL_ARRAY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, BOOLEAN, NONE),
                Arguments.of(NEAREST_NEIGHBORS, BOOLEAN_ARRAY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, UNKNOWN, NONE),
                Arguments.of(NEAREST_NEIGHBORS, NO_CATEGORY, NONE),
                Arguments.of(NEAREST_NEIGHBORS, ANYTHING, NONE));
    }

    private static Stream<Arguments> supportedValueCategories() {
        return Stream.of(
                Arguments.of(RANGE, ValueCategory.values()),
                Arguments.of(POINT, new ValueCategory[] {GEOMETRY}),
                Arguments.of(TEXT, new ValueCategory[] {ValueCategory.TEXT}),
                Arguments.of(TRIGRAM, new ValueCategory[] {ValueCategory.TEXT}),
                Arguments.of(VECTOR_V1, new ValueCategory[] {NUMBER_ARRAY}),
                Arguments.of(VECTOR_V2, new ValueCategory[] {NUMBER_ARRAY}),
                Arguments.of(TOKEN, new ValueCategory[] {}),
                Arguments.of(FULLTEXT, new ValueCategory[] {ValueCategory.TEXT, TEXT_ARRAY}));
    }

    private static IndexCapability[] of(IndexCapability... capabilities) {
        return capabilities;
    }
}
