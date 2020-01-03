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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.attribution.{Id, IdGen, SameId}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, LabelId, NonEmptyList, PropertyKeyId}

class IndexSeekTest extends CypherFunSuite {

  implicit val idGen: IdGen = SameId(Id(42))
  private val pos = InputPosition.NONE

  def createSeek(idName: String,
                 label: LabelToken,
                 properties: Seq[IndexedProperty],
                 valueExpr: QueryExpression[Expression],
                 argumentIds: Set[String],
                 indexOrder: IndexOrder): Boolean => IndexSeekLeafPlan = { unique =>
    if (unique) {
      NodeUniqueIndexSeek(idName, label, properties, valueExpr, argumentIds, indexOrder)
    } else {
      NodeIndexSeek(idName, label, properties, valueExpr, argumentIds, indexOrder)
    }
  }

  val testCaseCreators: List[(GetValueFromIndexBehavior, Set[String], IndexOrder) => (String, Boolean => LogicalPlan)] = List(
    (getValue, args, indexOrder) => "a:X(prop = 1)" -> createSeek("a", label("X"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:X(prop = 1)" -> createSeek("b", label("X"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(prop = 1)" -> createSeek("b", label("Y"), Seq(prop("prop", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 1)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue)), exactInt(1), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue)), exactInt(2), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 5)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue)), exactInts(2, 5), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2, cats = 4)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInt(2), exactInt(4))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 5, cats = 4)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInts(2, 5), exactInt(4))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2, cats = 4 OR 5)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInt(2), exactInts(4, 5))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 2 OR 3, cats = 4 OR 5)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInts(2, 3), exactInts(4, 5))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name = 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), exactString("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name < 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), lt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name <= 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), lte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name > 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), gt(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name >= 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), gte(string("hi")), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs < 3, cats >= 4)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(lt(intLiteral(3)), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(dogs = 3, cats >= 4)" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(exactInt(3), gte(intLiteral(4)))), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name STARTS WITH 'hi')" -> createSeek("b", label("Y"), Seq(prop("name", getValue)), startsWith("hi"), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name STARTS WITH 'hi', cats)" -> createSeek("b", label("Y"), Seq(prop("name", getValue, 0), prop("cats", getValue, 1)), CompositeQueryExpression(Seq(startsWith("hi"), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name ENDS WITH 'hi')" -> (_ => NodeIndexEndsWithScan("b", label("Y"), prop("name", getValue), string("hi"), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(dogs = 1, name ENDS WITH 'hi')" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("name", getValue, 1)), CompositeQueryExpression(Seq(exactInt(1), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name ENDS WITH 'hi', dogs = 1)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue, 0), prop("dogs", getValue, 1)), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(name CONTAINS 'hi')" -> (_ => NodeIndexContainsScan("b", label("Y"), prop("name", getValue), string("hi"), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(dogs = 1, name CONTAINS 'hi')" -> createSeek("b", label("Y"), Seq(prop("dogs", getValue, 0), prop("name", getValue, 1)), CompositeQueryExpression(Seq(exactInt(1), exists())), args, indexOrder),
    (getValue, args, indexOrder) => "b:Y(name CONTAINS 'hi', dogs)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue, 0), prop("dogs", getValue, 1)), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(name)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue)), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(name, dogs)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue, 0), prop("dogs", getValue, 1)), args, indexOrder)),
    (getValue, args, indexOrder) => "b:Y(name, dogs = 3)" -> (_ => NodeIndexScan("b", label("Y"), Seq(prop("name", getValue, 0), prop("dogs", getValue, 1)), args, indexOrder))
  )

  for {
    getValue <- List(CanGetValue, GetValue, DoNotGetValue)
    args <- List(Set.empty[String], Set("n", "m"))
    order <- List(IndexOrderNone, IndexOrderAscending, IndexOrderDescending)
    unique <- List(true, false)
    (str, expectedPlan) <- testCaseCreators.map(f => f(getValue, args, order))
  } {
    test(s"[$getValue, args=$args, order=$order, unique=$unique] should parse `$str`") {
      IndexSeek(str, getValue, argumentIds = args, indexOrder=order, unique=unique) should be(expectedPlan(unique))
    }
  }

  test("custom value expression") {
    IndexSeek("a:X(prop = ???)", paramExpr = Some(string("101"))) should be(
      NodeIndexSeek("a", label("X"), Seq(prop("prop", DoNotGetValue)), exactString("101"), Set.empty, IndexOrderNone)
    )
  }

  test("custom query expression") {
    IndexSeek("a:X(prop)", customQueryExpression = Some(exactInts(1, 2, 3)) ) should be(
      NodeIndexSeek("a", label("X"), Seq(prop("prop", DoNotGetValue)), exactInts(1, 2, 3), Set.empty, IndexOrderNone)
    )
  }

  // HELPERS

  private def label(str:String) = LabelToken(str, LabelId(0))

  private def prop(name: String, getValue: GetValueFromIndexBehavior, propId: Int = 0) =
    IndexedProperty(PropertyKeyToken(name, PropertyKeyId(propId)), getValue)

  private def exactInt(int: Int) = SingleQueryExpression(intLiteral(int))

  private def exactInts(ints: Int*) = ManyQueryExpression(ListLiteral(ints.map(i => intLiteral(i)))(pos))

  private def exactString(x: String) = SingleQueryExpression(string(x))

  private def string(x: String) = StringLiteral(x)(pos)
  private def intLiteral(x: Int) = SignedDecimalIntegerLiteral(x.toString)(pos)

  private def lt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def lte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(x))))(pos))
  private def gt(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(x))))(pos))
  private def gte(x: Literal) = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(x))))(pos))

  private def startsWith(x: String) = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(string(x)))(pos))

  private def exists() = ExistenceQueryExpression()

}
