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
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.ResultList;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

class VectorSSFGenericTest extends VectorSSFTestBase {

    @Test
    void unfiltered() throws Exception {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME);
        createTestNode(Map.of("id", 1, "age", 75, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        createTestNode(Map.of("id", 2, "name", "Osbert", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        createTestNode(Map.of("id", 3, "rating", 7.83, EMBEDDING_NAME, EMBEDDINGS.get(3)));

        ResultList result = queryNodeIndex();
        assertThat(result).hasSize(3);
    }

    @Test
    void unfilteredAndFiltered() throws Exception {
        // in order to break the index id ordering being 1,2,3
        createTestNode(Map.of("id", 103, "priority", 15, "story", "Once upon a time"));

        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age", "story");
        createTestNode(Map.of("id", 10, "age", 75, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        createTestNode(Map.of("id", 15, "story", "In a land far away", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        createTestNode(
                Map.of("id", 20, "story", "There lived a dragon", "age", 1000, EMBEDDING_NAME, EMBEDDINGS.get(3)));
        createTestNode(Map.of("id", 30, "name", "Puff", "age", 500, EMBEDDING_NAME, EMBEDDINGS.get(4)));
        createTestNode(Map.of("id", 40, "rating", 7.83, EMBEDDING_NAME, EMBEDDINGS.get(5)));

        ResultList unfiltered = queryNodeIndex();
        assertThat(unfiltered).hasSize(5);

        ResultList filtered = queryNodeIndex(exactQuery("age", Values.of(1000)));
        assertThat(filtered)
                .singleElement()
                .extracting(result -> result.getValue("story"))
                .isEqualTo(Values.of("There lived a dragon"));
    }

    @Test
    void invalidFilterQueryType() {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        createTestNode(Map.of("id", 10, "name", "Alice", "age", 23, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        assertThatThrownBy(() -> queryNodeIndex(token -> PropertyIndexQuery.fulltextSearch("Al")))
                .isInstanceOf(IndexNotApplicableKernelException.class)
                .hasMessageContainingAll(
                        "Tried to query a",
                        IndexType.VECTOR.name(),
                        "index with a query predicate which is not an accepted filter type",
                        "Invalid filter type was",
                        IndexQueryType.FULLTEXT_SEARCH.name());
    }

    @Test
    void invalidFilterQueryValue() {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        createTestNode(Map.of("id", 10, "name", "Alice", "age", 23, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        // GQL status assertion allows consistent checks of different Java exception types in SPD and non-SPD context
        assertThatThrownBy(() -> queryNodeIndex(
                        exactQuery("age", Values.pointValue(CoordinateReferenceSystem.CARTESIAN, -45, 75))))
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N01)
                .hasStatusDescriptionContaining("Expected the value")
                .hasStatusDescriptionContaining(
                        "to be of type INTEGER, FLOAT, STRING, BOOLEAN, DATE, LOCAL TIME, ZONED TIME, LOCAL DATETIME, ZONED DATETIME or DURATION")
                .hasStatusDescriptionContaining("but was of type POINT");
    }

    @Test
    void noSuchIndex() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> queryNodeIndex(exactQuery("age", Values.of(85.0f))))
                .isInstanceOf(TestException.class)
                .hasMessageContaining("This is not the expected index");
    }

    @Test
    void failOnNullFilter() {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", "age");
        assertThatThrownBy(() -> queryNodeIndex(allQuery("name"), null))
                .isInstanceOf(IndexNotApplicableKernelException.class)
                .hasMessageContainingAll(
                        "Tried to query a",
                        IndexType.VECTOR.name(),
                        "index with a query predicate which is not an accepted filter type",
                        "Invalid filter type was",
                        "null");
    }
}
