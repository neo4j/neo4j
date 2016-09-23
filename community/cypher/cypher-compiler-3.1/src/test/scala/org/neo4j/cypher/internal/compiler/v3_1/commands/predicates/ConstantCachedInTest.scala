/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.predicates

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite


class ConstantCachedInTest extends CachedInTest {
  override def createPredicate(lhs: Variable, rhs: Expression): Predicate = ConstantCachedIn(lhs, rhs)
}
class DynamicConstantInTest extends CachedInTest {
  override def createPredicate(lhs: Variable, rhs: Expression): Predicate = DynamicCachedIn(lhs, rhs)
}

abstract class CachedInTest extends CypherFunSuite {

  protected def createPredicate(lhs: Variable, rhs: Expression): Predicate

  test("tests") {
    // given
    val predicate = createPredicate(Variable("x"), ListLiteral(Literal(1), Literal(2), Literal(3)))

    implicit val state = QueryStateHelper.empty

    val v1 = ExecutionContext.empty.newWith("x" -> 1)
    val vNull = ExecutionContext.empty.newWith("x" -> null)
    val v14 = ExecutionContext.empty.newWith("x" -> 14)

    // then when
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(Some(false))

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(Some(false))
  }

  test("check with a collection containing null") {
    // given
    val predicate = createPredicate(Variable("x"), ListLiteral(Literal(1), Literal(2), Literal(null)))

    implicit val state = QueryStateHelper.empty

    val v1 = ExecutionContext.empty.newWith("x" -> 1)
    val vNull = ExecutionContext.empty.newWith("x" -> null)
    val v14 = ExecutionContext.empty.newWith("x" -> 14)

    // then when
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(None)

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(None)
  }

  test("check with a collection that is null") {
    // given
    val predicate = createPredicate(Variable("x"), Literal(null))

    implicit val state = QueryStateHelper.empty

    val v1 = ExecutionContext.empty.newWith("x" -> 1)
    val vNull = ExecutionContext.empty.newWith("x" -> null)
    val v14 = ExecutionContext.empty.newWith("x" -> 14)

    // then when
    predicate.isMatch(v1) should equal(None)
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(None)

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1) should equal(None)
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(None)
  }

  test("check lists") {
    // given
    val listInList = ListLiteral(
      ListLiteral(Literal(1), Literal(2)),
      ListLiteral(Literal(3), Literal(4)))
    val predicate = createPredicate(Variable("x"), listInList)

    implicit val state = QueryStateHelper.empty

    val v1 = ExecutionContext.empty.newWith("x" -> Seq(1,2))
    val vNull = ExecutionContext.empty.newWith("x" -> null)
    val v14 = ExecutionContext.empty.newWith("x" -> 14)

    // then when
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(Some(false))

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1) should equal(Some(true))
    predicate.isMatch(vNull) should equal(None)
    predicate.isMatch(v14) should equal(Some(false))
  }
}
