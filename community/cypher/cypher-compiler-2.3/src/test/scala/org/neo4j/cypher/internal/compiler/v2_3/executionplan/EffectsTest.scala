/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class EffectsTest extends CypherFunSuite {

  test("logical AND works") {
    val first = Effects(WritesRelationships, ReadsGivenNodeProperty("2"))
    val second = Effects(WritesRelationships, WritesGivenNodeProperty("2"))

    (first & second) should be(Effects(WritesRelationships))
  }

  test("logical AND works for write effects") {
    val first = AllWriteEffects
    val second = Effects(WritesRelationships, ReadsRelationships, WritesNodesWithLabels("foo"), WritesAnyNode)

    (first & second) should be(Effects(WritesRelationships, WritesAnyNode))
  }

  test("logical AND works for read effects") {
    val first = AllReadEffects
    val second = Effects(ReadsAllNodes, ReadsAnyNodeProperty, WritesGivenNodeProperty("bar"))

    (first & second) should be(Effects(ReadsAllNodes, ReadsAnyNodeProperty))
  }

  test("logical AND considers equal property names") {
    val first = Effects(WritesRelationships, ReadsGivenNodeProperty("foo"))
    val second = Effects(ReadsGivenNodeProperty("foo"))

    (first & second).effectsSet should contain only ReadsGivenNodeProperty("foo")
  }

  test("logical AND considers equal label names") {
    val first = Effects(ReadsAllNodes, ReadsNodesWithLabels("bar"))
    val second = Effects(ReadsNodesWithLabels("bar"))

    (first & second).effectsSet should contain only ReadsNodesWithLabels("bar")
  }

  test("logical OR works") {
    val first = Effects(WritesRelationships, WritesAnyNode, ReadsNodesWithLabels("foo"))
    val second = Effects(ReadsNodesWithLabels("foo"), WritesGivenNodeProperty("bar"))

    (first | second) should be(Effects(WritesRelationships, WritesAnyNode, ReadsNodesWithLabels("foo"), WritesGivenNodeProperty("bar")))
  }

  test("logical OR works for write effects") {
    val first = AllWriteEffects
    val second = Effects(WritesRelationships, ReadsRelationships, ReadsAllNodes)

    val expected = Effects(
      WritesAnyNode, WritesRelationships, WritesAnyNode, WritesAnyNodeProperty, WritesAnyRelationshipProperty, ReadsRelationships, ReadsAllNodes
    )

    (first | second) should be(expected)
  }

  test("logical OR works for read effects") {
    val first = AllReadEffects
    val second = Effects(ReadsGivenNodeProperty("foo"), WritesGivenNodeProperty("bar"))

    val expected = Effects(
      ReadsAllNodes, ReadsRelationships, ReadsAnyNodeProperty, ReadsAnyRelationshipProperty, ReadsAllNodes, ReadsGivenNodeProperty("foo"), WritesGivenNodeProperty("bar")
    )

    (first | second) should be(expected)
  }

  test("logical OR considers equal property names") {
    val first = Effects(WritesAnyNode, ReadsGivenNodeProperty("foo"))
    val second = Effects(ReadsGivenNodeProperty("foo"))

    (first | second) should be(Effects(WritesAnyNode, ReadsGivenNodeProperty("foo")))
  }

  test("logical OR considers equal label names") {
    val first = Effects(WritesAnyNode, ReadsNodesWithLabels("bar"))
    val second = Effects(ReadsNodesWithLabels("bar"))

    (first | second) should be(Effects(WritesAnyNode, ReadsNodesWithLabels("bar")))
  }
}
