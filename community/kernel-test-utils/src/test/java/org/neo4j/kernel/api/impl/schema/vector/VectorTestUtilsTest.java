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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Value;

class VectorTestUtilsTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.kernel.api.impl.schema.vector.VectorTestUtils#validVectorsFromValue")
    final void validVectorCandidate(Value candidate) {
        assertThat(VectorUtils.maybeToValidVector(candidate))
                .as("valid vector candidate should return value")
                .isNotNull();
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.kernel.api.impl.schema.vector.VectorTestUtils#invalidVectorsFromValue")
    final void invalidVectorCandidate(Value candidate) {
        assertThat(VectorUtils.maybeToValidVector(candidate))
                .as("invalid vector candidate should return null")
                .isNull();
    }
}
