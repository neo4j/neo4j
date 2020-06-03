/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.virtual.VirtualValues

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
    val predicate = createPredicate(Variable("x"), ListLiteral(literal(1), literal(2), literal(3)))

    val state = QueryStateHelper.empty

    val v1 = CypherRow.empty.copyWith("x", intValue(1))
    val vNull = CypherRow.empty.copyWith("x", NO_VALUE)
    val v14 = CypherRow.empty.copyWith("x", intValue(14))

    // then when
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(Some(false))

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(Some(false))
  }

  test("check with a collection containing null") {
    // given
    val predicate = createPredicate(Variable("x"), ListLiteral(literal(1), literal(2), Literal(NO_VALUE)))

    val state = QueryStateHelper.empty
    val v1 = CypherRow.empty.copyWith("x", intValue(1))
    val vNull = CypherRow.empty.copyWith("x", NO_VALUE)
    val v14 = CypherRow.empty.copyWith("x", intValue(14))

    // then when
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(None)

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(None)
  }

  test("check with a collection that is null") {
    // given
    val predicate = createPredicate(Variable("x"), literal(NO_VALUE))

    val state = QueryStateHelper.empty


    val v1 = CypherRow.empty.copyWith("x", intValue(1))
    val vNull = CypherRow.empty.copyWith("x", NO_VALUE)
    val v14 = CypherRow.empty.copyWith("x", intValue(14))

    // then when
    predicate.isMatch(v1, state) should equal(None)
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(None)

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1, state) should equal(None)
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(None)
  }

  test("check lists") {
    // given
    val listInList = ListLiteral(
      ListLiteral(literal(1), literal(2)),
      ListLiteral(literal(3), literal(4)))
    val predicate = createPredicate(Variable("x"), listInList)

    val state = QueryStateHelper.empty

    val v1 = CypherRow.empty.copyWith("x", VirtualValues.list(intValue(1), intValue(2)))
    val vNull = CypherRow.empty.copyWith("x", NO_VALUE)
    val v14 = CypherRow.empty.copyWith("x", intValue(14))

    // then when
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(Some(false))

    // and twice, just to check that the cache does not mess things up
    predicate.isMatch(v1, state) should equal(Some(true))
    predicate.isMatch(vNull, state) should equal(None)
    predicate.isMatch(v14, state) should equal(Some(false))
  }
}
