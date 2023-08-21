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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.core.RelationshipEntity
import org.neo4j.kernel.impl.coreapi.InternalTransaction

class ShortestPathNoDuplicatesTest extends CypherFunSuite {

  test("Should handle empty list") {
    ShortestPathExpression.noDuplicates(List.empty) should be(true)
  }

  test("Should handle lists of one") {
    ShortestPathExpression.noDuplicates(List(mock[Relationship])) should be(true)
  }

  test("Should handle lists of length two") {
    val transaction = mock[InternalTransaction]
    val a = new RelationshipEntity(transaction, 1)
    val b = new RelationshipEntity(transaction, 2)
    val a1 = new RelationshipEntity(transaction, 1)

    ShortestPathExpression.noDuplicates(List(a, b)) should be(true)
    ShortestPathExpression.noDuplicates(List(a, a)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, a1)) should be(false)
  }

  test("Should handle lists of length three") {
    val transaction = mock[InternalTransaction]
    val a = new RelationshipEntity(transaction, 1)
    val b = new RelationshipEntity(transaction, 2)
    val c = new RelationshipEntity(transaction, 3)

    ShortestPathExpression.noDuplicates(List(a, b, c)) should be(true)
    ShortestPathExpression.noDuplicates(List(a, a, b)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, b, b)) should be(false)
    ShortestPathExpression.noDuplicates(List(a, b, a)) should be(false)
  }

  test("Should handle long lists") {
    val transaction = mock[InternalTransaction]
    val a = new RelationshipEntity(transaction, 1)
    val b = new RelationshipEntity(transaction, 2)
    val c = new RelationshipEntity(transaction, 3)
    val d = new RelationshipEntity(transaction, 4)
    val e = new RelationshipEntity(transaction, 5)
    val f = new RelationshipEntity(transaction, 6)
    val g = new RelationshipEntity(transaction, 7)
    val h = new RelationshipEntity(transaction, 8)
    val i = new RelationshipEntity(transaction, 9)

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
