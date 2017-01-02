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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.commons.CypherFunSuite

class EffectsTest extends CypherFunSuite {

  test("logical AND works") {
    val first = Effects(WritesRelationships, ReadsNodeProperty("2"))
    val second = Effects(WritesRelationships, WritesNodeProperty("2"))

    (first & second) should be(Effects(WritesRelationships))
  }

  test("logical AND works for write effects") {
    val first = AllWriteEffects
    val second = Effects(WritesRelationships, ReadsRelationships, WritesLabel("foo"), WritesNodes)

    (first & second) should be(Effects(WritesRelationships, WritesNodes))
  }

  test("logical AND works for read effects") {
    val first = AllReadEffects
    val second = Effects(ReadsNodes, ReadsAnyNodeProperty, WritesNodeProperty("bar"))

    (first & second) should be(Effects(ReadsNodes, ReadsAnyNodeProperty))
  }

  test("logical AND considers equal property names") {
    val first = Effects(WritesRelationships, ReadsNodeProperty("foo"))
    val second = Effects(ReadsNodeProperty("foo"))

    (first & second).effectsSet should contain only ReadsNodeProperty("foo")
  }

  test("logical AND considers equal label names") {
    val first = Effects(ReadsNodes, ReadsLabel("bar"))
    val second = Effects(ReadsLabel("bar"))

    (first & second).effectsSet should contain only ReadsLabel("bar")
  }

  test("logical OR works") {
    val first = Effects(WritesRelationships, WritesNodes, ReadsLabel("foo"))
    val second = Effects(ReadsLabel("foo"), WritesNodeProperty("bar"))

    (first | second) should be(Effects(WritesRelationships, WritesNodes, ReadsLabel("foo"), WritesNodeProperty("bar")))
  }

  test("logical OR works for write effects") {
    val first = AllWriteEffects
    val second = Effects(WritesRelationships, ReadsRelationships, ReadsNodes)

    val expected = Effects(
      WritesNodes, WritesRelationships, WritesAnyLabel, WritesAnyNodeProperty, WritesAnyRelationshipProperty, ReadsRelationships, ReadsNodes
    )

    (first | second) should be(expected)
  }

  test("logical OR works for read effects") {
    val first = AllReadEffects
    val second = Effects(ReadsNodeProperty("foo"), WritesNodeProperty("bar"))

    val expected = Effects(
      ReadsNodes, ReadsRelationships, ReadsAnyNodeProperty, ReadsAnyRelationshipProperty, ReadsAnyLabel, ReadsNodeProperty("foo"), WritesNodeProperty("bar")
    )

    (first | second) should be(expected)
  }

  test("logical OR considers equal property names") {
    val first = Effects(WritesNodes, ReadsNodeProperty("foo"))
    val second = Effects(ReadsNodeProperty("foo"))

    (first | second) should be(Effects(WritesNodes, ReadsNodeProperty("foo")))
  }

  test("logical OR considers equal label names") {
    val first = Effects(WritesNodes, ReadsLabel("bar"))
    val second = Effects(ReadsLabel("bar"))

    (first | second) should be(Effects(WritesNodes, ReadsLabel("bar")))
  }
}
