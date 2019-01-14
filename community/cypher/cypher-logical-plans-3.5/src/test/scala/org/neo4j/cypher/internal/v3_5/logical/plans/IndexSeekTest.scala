/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.attribution.{Id, IdGen, SameId}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{InputPosition, LabelId, NonEmptyList, PropertyKeyId}

class IndexSeekTest extends CypherFunSuite {

  implicit val idGen: IdGen = SameId(Id(42))
  private val pos = InputPosition.NONE

  val testCaseCreators: List[(GetValueFromIndexBehavior, Set[String], IndexOrder) => (String, LogicalPlan)] = List(
    (getValue, args, indexOrder) => "a:X(prop = 1)" -> NodeIndexSeek("a", label("X"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:X(prop = 1)" -> NodeIndexSeek("b", label("X"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(prop = 1)" -> NodeIndexSeek("b", label("Y"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 1)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue)), exactInt(2), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2, cats = 4)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInt(2), exactInt(4))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name = 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), exactString("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name < 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), lt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name <= 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), lte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name > 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), gt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name >= 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), gte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name STARTS WITH 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", getValue)), startsWith("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name ENDS WITH 'hi')" -> NodeIndexEndsWithScan("b", label("Y"), prop("name", getValue), string("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name CONTAINS 'hi')" -> NodeIndexContainsScan("b", label("Y"), prop("name", getValue), string("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name)" -> NodeIndexScan("b", label("Y"), prop("name", getValue), args, indexOrder)
  )

  for {
    getValue <- List(CanGetValue, GetValue, DoNotGetValue)
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    (str, expectedPlan) <- testCaseCreators.map(f => f(getValue, args, order))
  } {
    test(s"[$getValue, args=$args, order=$order] should parse `$str`") {
      IndexSeek(str, getValue, argumentIds = args, indexOrder=order) should be(expectedPlan)
    }
  }

  test("custom value expression") {
    IndexSeek("a:X(prop = ???)", paramExpr = Some(string("101"))) should be(
      NodeIndexSeek("a", label("X"), Seq(prop("prop", DoNotGetValue)), exactString("101"), Set.empty, IndexOrderNone)
    )
  }

  // HELPERS

  private def label(str:String) = LabelToken(str, LabelId(0))

  private def prop(name: String, getValue: GetValueFromIndexBehavior, propId: Int = 0) =
    IndexedProperty(PropertyKeyToken(name, PropertyKeyId(propId)), getValue)

  private def exactInt(int: Int) = SingleQueryExpression(SignedDecimalIntegerLiteral(int.toString)(pos))

  private def exactString(x: String) = SingleQueryExpression(string(x))

  private def string(x: String) = StringLiteral(x)(pos)

  private def lt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def lte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(x))))(pos))
  private def gt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def gte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(x))))(pos))

  private def startsWith(x: String) = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(string(x)))(pos))

}
