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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.core.RelationshipProxy
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipActions

class ShortestPathNoDuplicatesTest extends CypherFunSuite {

  test("Should handle empty list") {
    ShortestPathExpression.noDuplicates(List.empty) should be(true)
  }

  test("Should handle lists of one") {
    ShortestPathExpression.noDuplicates(List(mock[Relationship])) should be(true)
  }

  test("Should handle lists of length two") {
    val actions = mock[RelationshipActions]
    val a = new RelationshipProxy(actions, 1)
    val b = new RelationshipProxy(actions, 2)
    val a1 = new RelationshipProxy(actions, 1)

    ShortestPathExpression.noDuplicates(List(a, b)) should be(true)
    ShortestPathExpression.noDuplicates(List(a, a)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, a1)) should be(false)
  }

  test("Should handle lists of length three") {
    val actions = mock[RelationshipActions]
    val a = new RelationshipProxy(actions, 1)
    val b = new RelationshipProxy(actions, 2)
    val c = new RelationshipProxy(actions, 3)

    ShortestPathExpression.noDuplicates(List(a, b, c)) should be(true)
    ShortestPathExpression.noDuplicates(List(a, a, b)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, b, b)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, b, a)) should be(false)
  }

  test("Should handle long lists") {
    val actions = mock[RelationshipActions]
    val a = new RelationshipProxy(actions, 1)
    val b = new RelationshipProxy(actions, 2)
    val c = new RelationshipProxy(actions, 3)
    val d = new RelationshipProxy(actions, 4)
    val e = new RelationshipProxy(actions, 5)
    val f = new RelationshipProxy(actions, 6)
    val g = new RelationshipProxy(actions, 7)
    val h = new RelationshipProxy(actions, 8)
    val i = new RelationshipProxy(actions, 9)

    val l0 = List(a, b, c, d, e, f, g, h, i)
    ShortestPathExpression.noDuplicates(l0) should be(true)
    val l1 = List(a, a, c, d, e, f, g, h, i)
    ShortestPathExpression.noDuplicates(l1) should be(false)
    val l2 = List(a, b, c, d, d, f, g, h, i)
    ShortestPathExpression.noDuplicates(l2) should be(false)
    val l3 = List(a, b, c, d, e, f, g, h, h)
    ShortestPathExpression.noDuplicates(l3) should be(false)
    val l4 = List(a, b, c, d, e, f, g, h, a)
    ShortestPathExpression.noDuplicates(l4) should be(false)
  }
}
