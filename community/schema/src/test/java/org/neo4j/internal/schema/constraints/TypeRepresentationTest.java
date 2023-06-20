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
package org.neo4j.internal.schema.constraints;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class TypeRepresentationTest {

    @Inject
    RandomSupport random;

    private static Stream<TypeRepresentation> enums() {
        return Stream.of(SchemaValueType.values());
    }

    @ParameterizedTest
    @MethodSource("enums")
    void testAllTypesHaveOrdering(TypeRepresentation type) {
        assertThat(TypeRepresentation.compare(type, type)).isEqualTo(0);
    }

    @Test
    void testCIP_100Ordering() {
        // GIVEN
        var entries = enums().collect(Collectors.toCollection(ArrayList<TypeRepresentation>::new));
        Collections.shuffle(entries, random.random());

        // WHEN
        var set = new TreeSet<>(TypeRepresentation::compare);
        set.addAll(entries);
        var actual = set.stream().map(TypeRepresentation::userDescription).toArray(String[]::new);

        // THEN
        var expected = new String[] {
            "BOOLEAN",
            "STRING",
            "INTEGER",
            "FLOAT",
            "DATE",
            "LOCAL TIME",
            "ZONED TIME",
            "LOCAL DATETIME",
            "ZONED DATETIME",
            "DURATION",
            "POINT",
        };
        assertThat(actual).isEqualTo(expected);
    }
}
