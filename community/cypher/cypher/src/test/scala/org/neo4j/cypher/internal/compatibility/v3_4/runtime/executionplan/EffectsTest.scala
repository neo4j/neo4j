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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

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
    Effects(CreatesAnyNode, ReadsAllRelationships).containsWrites shouldBe(right = true)
    Effects().containsWrites shouldBe(right = false)
    Effects(ReadsNodesWithLabels("foo", "bar")).containsWrites shouldBe(right = false)
  }

  test("testRegardlessOfOptionalEffects") {
    val actual = Effects(CreatesAnyNode, LeafEffect(DeletesNode), OptionalLeafEffect(SetLabel("Label"))).regardlessOfOptionalEffects
    val expected = Effects(CreatesAnyNode, LeafEffect(DeletesNode), SetLabel("Label"))

    actual should equal(expected)
  }

  test("testContains") {
    Effects().contains(DeletesRelationship) shouldBe(right = false)
    Effects(CreatesNodesWithLabels("foo")).contains(CreatesNodesWithLabels("bar")) shouldBe(right = false)
    Effects(CreatesAnyNode, DeletesRelationship).contains(CreatesAnyNode) shouldBe(right = true)
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
