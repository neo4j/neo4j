/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.attribution.{Id, IdGen, SameId}
import org.opencypher.v9_0.util.{InputPosition, LabelId, NonEmptyList, PropertyKeyId}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class IndexSeekTest extends CypherFunSuite {

  implicit val idGen: IdGen = SameId(Id(42))
  private val pos = InputPosition.NONE

  val testCaseCreators: List[GetValueFromIndexBehavior => (String, LogicalPlan)] = List(
    x => "a:X(prop = 1)" -> NodeIndexSeek("a", label("X"), Seq(prop("prop", x)), exactInt(1), Set.empty),
    x => "b:X(prop = 1)" -> NodeIndexSeek("b", label("X"), Seq(prop("prop", x)), exactInt(1), Set.empty),
    x => "b:Y(prop = 1)" -> NodeIndexSeek("b", label("Y"), Seq(prop("prop", x)), exactInt(1), Set.empty),
    x => "b:Y(dogs = 1)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", x)), exactInt(1), Set.empty),
    x => "b:Y(dogs = 2)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", x)), exactInt(2), Set.empty),
    x => "b:Y(dogs = 2, cats = 4)" -> NodeIndexSeek("b", label("Y"), Seq(prop("dogs", x), prop("cats", x)), CompositeQueryExpression(Seq(exactInt(2), exactInt(4))), Set.empty),
    x => "b:Y(name = 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), exactString("hi"), Set.empty),
    x => "b:Y(name < 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), lt(string("hi")), Set.empty),
    x => "b:Y(name <= 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), lte(string("hi")), Set.empty),
    x => "b:Y(name > 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), gt(string("hi")), Set.empty),
    x => "b:Y(name >= 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), gte(string("hi")), Set.empty),
    x => "b:Y(name STARTS WITH 'hi')" -> NodeIndexSeek("b", label("Y"), Seq(prop("name", x)), startsWith("hi"), Set.empty),
    x => "b:Y(name ENDS WITH 'hi')" -> NodeIndexEndsWithScan("b", label("Y"), prop("name", x), string("hi"), Set.empty),
    x => "b:Y(name CONTAINS 'hi')" -> NodeIndexContainsScan("b", label("Y"), prop("name", x), string("hi"), Set.empty),
    x => "b:Y(name)" -> NodeIndexScan("b", label("Y"), prop("name", x), Set.empty)
  )

  for {
    getValue <- List(CanGetValue, GetValue, DoNotGetValue)
    (str, expectedPlan) <- testCaseCreators.map(f => f(getValue))
  } {
    test(s"[$getValue] should parse `$str`") {
      IndexSeek(str, getValue) should be(expectedPlan)
    }
  }

  // HELPERS

  private def label(str:String) = LabelToken(str, LabelId(0))

  private def prop(name: String, getValue: GetValueFromIndexBehavior) =
    IndexedProperty(PropertyKeyToken(name, PropertyKeyId(0)), getValue)

  private def exactInt(int: Int) = SingleQueryExpression(SignedDecimalIntegerLiteral(int.toString)(pos))

  private def exactString(x: String) = SingleQueryExpression(string(x))

  private def string(x: String) = StringLiteral(x)(pos)

  private def lt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def lte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(x))))(pos))
  private def gt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def gte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(x))))(pos))

  private def startsWith(x: String) = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(string(x)))(pos))

}
