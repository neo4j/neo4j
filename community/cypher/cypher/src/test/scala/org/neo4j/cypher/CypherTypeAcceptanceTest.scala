/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher

import org.junit.Test

class CypherTypeAcceptanceTest extends ExecutionEngineJUnitSuite {
  @Test def does_not_loose_precision() {
    // Given
    execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeScalar[Number]("match (p:Label) return p.id")

    assert(result === 4611686018427387905L)
  }

  @Test def equality_takes_the_full_value_into_consideration1() {
    // Given
    execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = execute("match (p:Label {id: 4611686018427387905}) return p")

    assert(result.nonEmpty, "Should find the node")
  }

  @Test def equality_takes_the_full_value_into_consideration2() {
    // Given
    execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = execute("match (p:Label) where p.id = 4611686018427387905 return p")

    assert(result.nonEmpty, "Should find the node")
  }

  @Test def equality_takes_the_full_value_into_consideration3() {
    // Given
    execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = execute("match (p:Label) where p.id = 4611686018427387900 return p")

    assert(result.isEmpty, "Should not find the node")
  }

  @Test def equality_takes_the_full_value_into_consideration4() {
    // Given
    execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = execute("match (p:Label {id : 4611686018427387900}) return p")

    assert(result.isEmpty, "Should not find the node")
  }
}
