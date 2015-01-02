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

class UniquenessAcceptanceTest extends ExecutionEngineJUnitSuite {

  @Test def should_not_reuse_the_relationship_that_has_just_been_traversed() {
    // Given
    relate(createNode("Me"), createNode("Bob"))

    // When
    val result = execute("MATCH a-->()-->b WHERE a.name = 'Me' RETURN b.name")

    // Then
    assert(List() === result.toList)
  }

  @Test def should_not_reuse_a_relationship_that_was_used_earlier() {
    // Given a graph: n1-->n2, n2-->n2
    val n1 = createNode("start")
    val n2 = createNode()
    relate(n1, n2)
    relate(n2, n2)

    // When
    val result = execute("match a--b-->c--d where a.name = 'start' return d")

    // Then
    assert(List() === result.toList)
  }

  @Test def should_reuse_relationships_that_were_used_in_a_different_clause() {
    // Given
    // leaf1-->parent
    // leaf2-->parent
    val leaf1 = createNode("leaf1")
    val leaf2 = createNode("leaf2")
    val parent = createNode("parent")
    relate(leaf1, parent)
    relate(leaf2, parent)

    // When
    val result = execute("MATCH x-->parent WHERE x.name = 'leaf1' WITH parent MATCH leaf-->parent RETURN leaf")

    // Then
    assert(2 === result.size)
  }

  @Test def should_reuse_relationships_even_though_they_are_in_context_but_not_used_in_a_pattern() {
    // Given
    // leaf1-->parent
    // leaf2-->parent
    val leaf1 = createNode("leaf1")
    val leaf2 = createNode("leaf2")
    val parent = createNode("parent")
    relate(leaf1, parent)
    relate(leaf2, parent)

    // When
    val result = execute("MATCH x-[r]->parent WHERE x.name = 'leaf1' WITH r, parent MATCH leaf-->parent RETURN r, leaf")

    // Then
    assert(2 === result.size)
  }
}
