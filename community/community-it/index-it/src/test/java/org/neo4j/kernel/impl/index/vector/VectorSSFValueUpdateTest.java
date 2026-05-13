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
import static org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.field;

import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

class VectorSSFValueUpdateTest extends VectorSSFTestBase {

    @Test
    void fromIntToFloat() throws Exception {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        createTestNode(Map.of("id", 1, "age", 75, EMBEDDING_NAME, EMBEDDINGS.get(1)));

        updateTestNode("id", 1, Map.of("age", 85.0f));
        assertThat(queryNodeIndex(exactQuery("age", Values.of(75)))).isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(Math.nextDown(85.0f)))))
                .isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(85.0f))))
                .singleElement()
                .extracting(result -> result.getValue("age"))
                .isEqualTo(Values.of(85.0f));
        assertThat(queryNodeIndex(exactQuery("age", Values.of(Math.nextUp(85.0f)))))
                .isEmpty();

        updateTestNode("id", 1, Map.of("age", 90.0f));
        assertThat(queryNodeIndex(exactQuery("age", Values.of(85.0f)))).isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(90.0f)))).hasSize(1);
        assertThat(queryNodeIndex(exactQuery("age", Values.of(90))))
                .as("int 90 should match float 90.0f")
                .singleElement();
    }

    @Test
    void fromFloatToInt() throws Exception {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        createTestNode(Map.of("id", 1, "age", 75.0f, EMBEDDING_NAME, EMBEDDINGS.get(1)));

        updateTestNode("id", 1, Map.of("age", 85));
        assertThat(queryNodeIndex(exactQuery("age", Values.of(75.0f)))).isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(85)))).hasSize(1);

        updateTestNode("id", 1, Map.of("age", 90));
        assertThat(queryNodeIndex(exactQuery("age", Values.of(85)))).isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(90)))).hasSize(1);
        assertThat(queryNodeIndex(exactQuery("age", Values.of(Math.nextDown(90.0f)))))
                .isEmpty();
        assertThat(queryNodeIndex(exactQuery("age", Values.of(90.0f))))
                .as("float 90.0f should match int 90")
                .hasSize(1);
        assertThat(queryNodeIndex(exactQuery("age", Values.of(Math.nextUp(90.0f)))))
                .isEmpty();
    }

    @Test
    void fromIndexableFieldToNonIndexableField() throws Exception {
        String officeName = "Malmö Office";
        Point officeLocation = Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.994840, 55.612103);
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "location");

        createTestNode(Map.of("id", 1, "name", "Bob", "location", officeName, EMBEDDING_NAME, EMBEDDINGS.get(5)));
        assertThat(queryNodeIndex(existsQuery("location"))).singleElement().has(field("location", officeName));

        updateTestNode("id", 1, Map.of("location", officeLocation));
        assertThat(queryNodeIndex(existsQuery("location"))).singleElement().has(field("location", officeLocation));
    }

    @Test
    void fromNonIndexableFieldToIndexableField() throws Exception {
        String officeName = "Malmö Office";
        Point officeLocation = Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.994840, 55.612103);
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "location");

        createTestNode(Map.of("id", 1, "name", "Bob", "location", officeLocation, EMBEDDING_NAME, EMBEDDINGS.get(5)));
        assertThat(queryNodeIndex(existsQuery("location"))).singleElement().has(field("location", officeLocation));

        updateTestNode("id", 1, Map.of("location", officeName));
        assertThat(queryNodeIndex(existsQuery("location"))).singleElement().has(field("location", officeName));
    }

    @Test
    void addAndRemoveValue() throws Exception {
        String officeName = "Malmö Office";
        Point officeLocation = Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.994840, 55.612103);
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age", "location");

        createTestNode(Map.of("id", 1, "name", "Alice", "age", 23, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        assertThat(queryNodeIndex()).hasSize(1);
        assertThat(queryNodeIndex(allQuery("age"), allQuery("location"))).hasSize(1);
        assertThat(queryNodeIndex(allQuery("age"), existsQuery("location"))).isEmpty();
        assertThat(queryNodeIndex(allQuery("age"), notExistsQuery("location"))).hasSize(1);

        updateTestNode("id", 1, Map.of("location", officeName));
        assertThat(queryNodeIndex(allQuery("age"), existsQuery("location"))).hasSize(1);
        assertThat(queryNodeIndex(allQuery("age"), notExistsQuery("location"))).isEmpty();

        updateTestNode("id", 1, Maps.mutable.of("location", null));
        assertThat(queryNodeIndex(allQuery("age"), allQuery("location"))).hasSize(1);
        assertThat(queryNodeIndex(allQuery("age"), existsQuery("location"))).isEmpty();
        assertThat(queryNodeIndex(allQuery("age"), notExistsQuery("location"))).hasSize(1);

        updateTestNode("id", 1, Map.of("location", officeLocation));
        assertThat(queryNodeIndex(allQuery("age"), existsQuery("location"))).hasSize(1);
        assertThat(queryNodeIndex(allQuery("age"), notExistsQuery("location"))).isEmpty();

        deleteTestNode("id", 1);
        assertThat(queryNodeIndex(allQuery("age"), allQuery("location"))).isEmpty();
        assertThat(queryNodeIndex()).isEmpty();
    }

    @Test
    void unindexedField() throws Exception {
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", "age");
        createTestNode(Map.of(
                "id", 10, "name", "Alice", "age", 23, "nationality", "Slovenian", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        createTestNode(Map.of("id", 20, "name", "Bob", "age", 20, EMBEDDING_NAME, EMBEDDINGS.get(2)));

        assertThat(queryNodeIndex(allQuery("name"), exactQuery("age", Values.of(23))))
                .singleElement()
                .has(field("nationality", "Slovenian"))
                .has(field("name", "Alice"));

        updateTestNode("id", 10, Map.of("nationality", "Croatian"));

        assertThat(queryNodeIndex(allQuery("name"), exactQuery("age", Values.of(23))))
                .singleElement()
                .has(field("nationality", "Croatian"))
                .has(field("name", "Alice"));
    }
}
