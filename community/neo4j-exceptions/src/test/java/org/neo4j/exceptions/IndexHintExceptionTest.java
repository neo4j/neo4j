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
package org.neo4j.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.exceptions.IndexHintException.IndexHintIndexType.ANY;
import static org.neo4j.exceptions.IndexHintException.IndexHintIndexType.POINT;
import static org.neo4j.exceptions.IndexHintException.IndexHintIndexType.RANGE;
import static org.neo4j.exceptions.IndexHintException.IndexHintIndexType.TEXT;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;

class IndexHintExceptionTest {
    @ParameterizedTest(name = "Hint on {0}, {1}, {2}, {3}, {4} is shown as {5}")
    @MethodSource("testCases")
    void indexFormatStringTest(
            String variableName,
            String labelOrRelType,
            List<String> properties,
            EntityType entityType,
            IndexHintIndexType indexType,
            String expected) {
        String actual =
                IndexHintException.indexFormatString(variableName, labelOrRelType, properties, entityType, indexType);
        assertEquals(expected, actual);
    }

    static Stream<Arguments> testCases() {
        return Stream.of(
                basicHint(NODE, ANY, "INDEX FOR (`person`:`Person`) ON (`person`.`name`)"),
                basicHint(NODE, TEXT, "TEXT INDEX FOR (`person`:`Person`) ON (`person`.`name`)"),
                basicHint(NODE, RANGE, "RANGE INDEX FOR (`person`:`Person`) ON (`person`.`name`)"),
                basicHint(NODE, POINT, "POINT INDEX FOR (`person`:`Person`) ON (`person`.`name`)"),
                basicHint(RELATIONSHIP, ANY, "INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)"),
                basicHint(RELATIONSHIP, TEXT, "TEXT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)"),
                basicHint(RELATIONSHIP, RANGE, "RANGE INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)"),
                basicHint(RELATIONSHIP, POINT, "POINT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)"),
                compositeHint(NODE, ANY, "INDEX FOR (`person`:`Person`) ON (`person`.`name`, `person`.`surname`)"),
                compositeHint(
                        RELATIONSHIP,
                        ANY,
                        "INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`, `person`.`surname`)"),
                escapedHint(
                        NODE,
                        ANY,
                        "INDEX FOR (`pers``on`:`Pers``on`) ON (`pers``on`.`nam``e`, `pers``on`.`s``urname`)"),
                escapedHint(
                        RELATIONSHIP,
                        ANY,
                        "INDEX FOR ()-[`pers``on`:`Pers``on`]-() ON (`pers``on`.`nam``e`, `pers``on`.`s``urname`)"));
    }

    static Arguments basicHint(EntityType entityType, IndexHintIndexType indexType, String expected) {
        return Arguments.of("person", "Person", Collections.singletonList("name"), entityType, indexType, expected);
    }

    static Arguments compositeHint(EntityType entityType, IndexHintIndexType indexType, String expected) {
        return Arguments.of("person", "Person", Arrays.asList("name", "surname"), entityType, indexType, expected);
    }

    static Arguments escapedHint(EntityType entityType, IndexHintIndexType indexType, String expected) {
        return Arguments.of("pers`on", "Pers`on", Arrays.asList("nam`e", "s`urname"), entityType, indexType, expected);
    }
}
