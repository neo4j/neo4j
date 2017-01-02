/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class EffectsTest extends CypherFunSuite {

  test("testAsLeafEffects") {
    val actual: Effects = Effects(CreatesAnyNode, ReadsAllRelationships).asLeafEffects
    val expected: Effects = Effects(LeafEffect(CreatesAnyNode), LeafEffect(ReadsAllRelationships))

    actual should equal(expected)
  }

  test("testLeafEffectsAsOptional") {
    val actual: Effects = Effects(LeafEffect(CreatesAnyNode), LeafEffect(ReadsAllRelationships)).leafEffectsAsOptional
    val expected: Effects = Effects(OptionalLeafEffect(CreatesAnyNode), OptionalLeafEffect(ReadsAllRelationships))

    actual should equal(expected)
  }

  test("testWriteEffects") {
    val actual: Effects = Effects(CreatesAnyNode, ReadsAllRelationships).writeEffects
    val expected: Effects = Effects(CreatesAnyNode)

    actual should equal(expected)
  }

  test("testContainsWrites") {
    Effects(CreatesAnyNode, ReadsAllRelationships).containsWrites should be(right = true)
    Effects().containsWrites should be(right = false)
    Effects(ReadsNodesWithLabels("foo", "bar")).containsWrites should be(right = false)
  }

  test("testRegardlessOfOptionalEffects") {
    val actual = Effects(CreatesAnyNode, LeafEffect(DeletesNode), OptionalLeafEffect(SetLabel("Label"))).regardlessOfOptionalEffects
    val expected = Effects(CreatesAnyNode, LeafEffect(DeletesNode), SetLabel("Label"))

    actual should equal(expected)
  }

  test("testContains") {
    Effects().contains(DeletesRelationship) should be(right = false)
    Effects(CreatesNodesWithLabels("foo")).contains(CreatesNodesWithLabels("bar")) should be(right = false)
    Effects(CreatesAnyNode, DeletesRelationship).contains(CreatesAnyNode) should be(right = true)
  }

  test("testRegardlessOfLeafEffects") {
    val expected: Effects = Effects(CreatesAnyNode, DeletesRelationship, OptionalLeafEffect(ReadsNodesWithLabels("foo")))
    val actual: Effects = Effects(CreatesAnyNode, LeafEffect(DeletesRelationship), OptionalLeafEffect(ReadsNodesWithLabels("foo"))).regardlessOfLeafEffects

    actual should equal(expected)
  }

  test("test$plus$plus") {
    Effects() ++ Effects() should equal(Effects())
    Effects(CreatesRelationshipBoundNodes) ++ Effects(CreatesRelationshipBoundNodes, DeletesNode) should equal(Effects(CreatesRelationshipBoundNodes, DeletesNode))
  }

}
