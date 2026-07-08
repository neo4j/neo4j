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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QueryExpressionStringifierTest extends CypherFunSuite with AstConstructionTestSupport {

  private val defaultStringifier = new QueryExpressionStringifier(ExpressionStringifier())
  private val cypherStringifier = new QueryExpressionStringifier(ExpressionStringifier(), dialect = Dialect.Cypher)

  // SingleQueryExpression tests
  test("should stringify SingleQueryExpression with literal") {
    val expr = SingleQueryExpression(literalInt(42))
    defaultStringifier(expr, Seq("prop")) should equal("prop = 42")
  }

  test("should stringify SingleQueryExpression with string literal") {
    val expr = SingleQueryExpression(literalString("hello"))
    defaultStringifier(expr, Seq("prop")) should equal("prop = \"hello\"")
  }

  test("should stringify SingleQueryExpression with parameter") {
    val expr = SingleQueryExpression(parameter("param", CTAny))
    defaultStringifier(expr, Seq("prop")) should equal("prop = $param")
  }

  // ManyQueryExpression tests
  test("should stringify ManyQueryExpression with ListLiteral as OR") {
    val expr = ManyQueryExpression(listOf(literalInt(1), literalInt(2), literalInt(3)))
    defaultStringifier(expr, Seq("prop")) should equal("prop = 1 OR 2 OR 3")
  }

  test("should stringify ManyQueryExpression with non-ListLiteral as IN") {
    val expr = ManyQueryExpression(parameter("list", CTAny))
    defaultStringifier(expr, Seq("prop")) should equal("prop IN $list")
  }

  test("should stringify ManyQueryExpression with variable as IN") {
    val expr = ManyQueryExpression(varFor("myList"))
    defaultStringifier(expr, Seq("prop")) should equal("prop IN myList")
  }

  test("should stringify ManyQueryExpression with ListLiteral as IN [list] under Dialect.Cypher") {
    val expr = ManyQueryExpression(listOf(literalInt(1), literalInt(2), literalInt(3)))
    cypherStringifier(expr, Seq("prop")) should equal("prop IN [1, 2, 3]")
  }

  test("should stringify ManyQueryExpression with empty ListLiteral as IN [] under Dialect.Cypher") {
    val expr = ManyQueryExpression(listOf())
    cypherStringifier(expr, Seq("prop")) should equal("prop IN []")
  }

  test("should stringify ManyQueryExpression with single-element ListLiteral as IN [x] under Dialect.Cypher") {
    val expr = ManyQueryExpression(listOf(literalInt(1)))
    cypherStringifier(expr, Seq("prop")) should equal("prop IN [1]")
  }

  test("should stringify ManyQueryExpression with map element under Dialect.Cypher") {
    val expr = ManyQueryExpression(listOf(mapOf("foo" -> literalInt(20)), literalInt(10)))
    cypherStringifier(expr, Seq("prop")) should equal("prop IN [{foo: 20}, 10]")
  }

  test("should stringify ManyQueryExpression with null element under Dialect.Cypher") {
    val expr = ManyQueryExpression(listOf(literalInt(10), nullLiteral))
    cypherStringifier(expr, Seq("prop")) should equal("prop IN [10, NULL]")
  }

  test("should stringify composite of many-value seeks as AND of IN-lists under Dialect.Cypher") {
    val expr = CompositeQueryExpression(Seq(
      ManyQueryExpression(listOf(literalInt(1), literalInt(2))),
      ManyQueryExpression(listOf(literalInt(3), literalInt(4)))
    ))
    cypherStringifier(expr, Seq("prop1", "prop2")) should equal("prop1 IN [1, 2] AND prop2 IN [3, 4]")
  }

  test("should stringify ManyQueryExpression with non-ListLiteral as unbracketed IN under Dialect.Cypher") {
    val expr = ManyQueryExpression(parameter("list", CTAny))
    cypherStringifier(expr, Seq("prop")) should equal("prop IN $list")
  }

  // ExistenceQueryExpression tests
  test("should stringify ExistenceQueryExpression") {
    val expr = ExistenceQueryExpression
    defaultStringifier(expr, Seq("prop")) should equal("prop")
  }

  // RangeQueryExpression tests - PrefixSeekRangeWrapper
  test("should stringify RangeQueryExpression with STARTS WITH literal") {
    val expr = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(literalString("abc")))(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop STARTS WITH \"abc\"")
  }

  test("should stringify RangeQueryExpression with STARTS WITH parameter") {
    val expr = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(parameter("prefix", CTAny)))(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop STARTS WITH $prefix")
  }

  // RangeQueryExpression tests - InequalitySeekRangeWrapper - RangeGreaterThan
  test("should stringify RangeQueryExpression with > (exclusive)") {
    val range = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop > 10")
  }

  test("should stringify RangeQueryExpression with >= (inclusive)") {
    val range = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(10))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop >= 10")
  }

  test("should stringify RangeQueryExpression with > parameter") {
    val range = RangeGreaterThan(NonEmptyList(ExclusiveBound(parameter("lower", CTInteger))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop > $lower")
  }

  // RangeQueryExpression tests - InequalitySeekRangeWrapper - RangeLessThan
  test("should stringify RangeQueryExpression with < (exclusive)") {
    val range = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(20))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop < 20")
  }

  test("should stringify RangeQueryExpression with <= (inclusive)") {
    val range = RangeLessThan(NonEmptyList(InclusiveBound(literalInt(20))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop <= 20")
  }

  test("should stringify RangeQueryExpression with < parameter") {
    val range = RangeLessThan(NonEmptyList(ExclusiveBound(parameter("upper", CTInteger))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop < $upper")
  }

  // RangeQueryExpression tests - InequalitySeekRangeWrapper - RangeBetween
  test("should stringify RangeQueryExpression with between (exclusive, exclusive)") {
    val gt = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(20))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 < prop < 20")
  }

  test("should stringify RangeQueryExpression with between (inclusive, inclusive)") {
    val gt = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(10))))
    val lt = RangeLessThan(NonEmptyList(InclusiveBound(literalInt(20))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 <= prop <= 20")
  }

  test("should stringify RangeQueryExpression with between (inclusive, exclusive)") {
    val gt = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(10))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(20))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 <= prop < 20")
  }

  test("should stringify RangeQueryExpression with between (exclusive, inclusive)") {
    val gt = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val lt = RangeLessThan(NonEmptyList(InclusiveBound(literalInt(20))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 < prop <= 20")
  }

  test("should stringify RangeQueryExpression with between using parameters") {
    val gt = RangeGreaterThan(NonEmptyList(ExclusiveBound(parameter("lower", CTInteger))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(parameter("upper", CTInteger))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("$lower < prop < $upper")
  }

  // CompositeQueryExpression tests
  test("should stringify CompositeQueryExpression with multiple properties") {
    val expr = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      SingleQueryExpression(literalString("abc"))
    ))
    defaultStringifier(expr, Seq("prop1", "prop2")) should equal("prop1 = 1, prop2 = \"abc\"")
  }

  test("should stringify CompositeQueryExpression with existence and range") {
    val range = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val expr = CompositeQueryExpression(Seq(
      ExistenceQueryExpression,
      RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    ))
    defaultStringifier(expr, Seq("prop1", "prop2")) should equal("prop1, prop2 > 10")
  }

  // Custom valueStringifier tests
  test("should use custom valueStringifier when provided") {
    val customStringifier = new QueryExpressionStringifier(
      ExpressionStringifier(),
      Some((_: Expression) => "CUSTOM")
    )
    val expr = SingleQueryExpression(literalInt(42))
    customStringifier(expr, Seq("prop")) should equal("prop = CUSTOM")
  }

  test("should use custom valueStringifier for parameters (like LogicalPlanToPlanBuilderString)") {
    val customStringifier = new QueryExpressionStringifier(
      ExpressionStringifier(),
      Some {
        case _: ExplicitParameter => "???"
        case e                    => ExpressionStringifier()(e)
      }
    )
    val expr = SingleQueryExpression(parameter("param", CTAny))
    customStringifier(expr, Seq("prop")) should equal("prop = ???")
  }

  test("custom valueStringifier should apply to all expression types in range") {
    val customStringifier = new QueryExpressionStringifier(
      ExpressionStringifier(),
      Some {
        case _: ExplicitParameter => "???"
        case e                    => ExpressionStringifier()(e)
      }
    )
    val gt = RangeGreaterThan(NonEmptyList(ExclusiveBound(parameter("lower", CTInteger))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(parameter("upper", CTInteger))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    customStringifier(expr, Seq("prop")) should equal("??? < prop < ???")
  }

  // RangeGreaterThan with multiple bounds (used for different types)
  test("should stringify RangeGreaterThan with multiple bounds") {
    val range = RangeGreaterThan(NonEmptyList(
      ExclusiveBound(literalInt(10)),
      ExclusiveBound(literalString("foo"))
    ))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 < prop > \"foo\"")
  }

  test("should stringify RangeLessThan with multiple bounds") {
    val range = RangeLessThan(NonEmptyList(
      ExclusiveBound(literalInt(10)),
      ExclusiveBound(literalString("foo"))
    ))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("10 > prop < \"foo\"")
  }

  test("should stringify RangeBetween with multi-bound sub-ranges by emitting all bounds (PlanBuilder)") {
    val gt = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(0)), InclusiveBound(literalInt(5))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(10)), ExclusiveBound(literalInt(100))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(gt, lt))(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop >= 0 AND prop >= 5 AND prop < 10 AND prop < 100")
  }

  test("should stringify RangeBetween with multi-bound sub-ranges by emitting all bounds (Cypher)") {
    val gt = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(0)), InclusiveBound(literalInt(5))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(10)), ExclusiveBound(literalInt(100))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(gt, lt))(pos))
    cypherStringifier(expr, Seq("prop")) should equal("prop >= 0 AND prop >= 5 AND prop < 10 AND prop < 100")
  }

  test("should stringify RangeGreaterThan with three bounds by emitting all bounds") {
    val range = RangeGreaterThan(NonEmptyList(
      ExclusiveBound(literalInt(0)),
      ExclusiveBound(literalInt(5)),
      ExclusiveBound(literalInt(8))
    ))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop > 0 AND prop > 5 AND prop > 8")
  }

  test("should stringify RangeLessThan with three bounds by emitting all bounds") {
    val range = RangeLessThan(NonEmptyList(
      ExclusiveBound(literalInt(10)),
      ExclusiveBound(literalInt(100)),
      ExclusiveBound(literalInt(1000))
    ))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop < 10 AND prop < 100 AND prop < 1000")
  }

  test(
    "should stringify RangeBetween with a multi-bound sub-range and a single-bound sub-range by emitting all bounds"
  ) {
    val gt = RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(0)), InclusiveBound(literalInt(5))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(gt, lt))(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop >= 0 AND prop >= 5 AND prop < 10")
  }

  test("inequality-range rendering is identical across dialects (PlanBuilder == Cypher)") {
    // guard again future dialect-dependent changes to range rendering
    val ranges: Seq[InequalitySeekRange[Expression]] = Seq(
      // >2-bound conjunction branch (RangeGreaterThan / RangeLessThan)
      RangeGreaterThan(NonEmptyList(
        ExclusiveBound(literalInt(0)),
        ExclusiveBound(literalInt(5)),
        ExclusiveBound(literalInt(8))
      )),
      RangeLessThan(NonEmptyList(
        ExclusiveBound(literalInt(10)),
        ExclusiveBound(literalInt(100)),
        ExclusiveBound(literalInt(1000))
      )),
      // >2-bound conjunction branch (RangeBetween with a multi-bound sub-range)
      RangeBetween(
        RangeGreaterThan(NonEmptyList(InclusiveBound(literalInt(0)), InclusiveBound(literalInt(5)))),
        RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(10))))
      ),
      // chained <=2-bound path
      RangeBetween(
        RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10)))),
        RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(20))))
      )
    )
    ranges.foreach { range =>
      val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
      withClue(range.toString) {
        defaultStringifier(expr, Seq("prop")) should equal(cypherStringifier(expr, Seq("prop")))
      }
    }
  }

  test("should stringify RangeLessThan with three inclusive bounds by emitting all bounds (<=)") {
    val range = RangeLessThan(NonEmptyList(
      InclusiveBound(literalInt(10)),
      InclusiveBound(literalInt(100)),
      InclusiveBound(literalInt(1000))
    ))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, Seq("prop")) should equal("prop <= 10 AND prop <= 100 AND prop <= 1000")
  }

  // Tests with entity parameter
  test("should stringify SingleQueryExpression with entity") {
    val expr = SingleQueryExpression(parameter("param", CTAny))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop = $param")
  }

  test("should stringify SingleQueryExpression with entity and literal") {
    val expr = SingleQueryExpression(literalInt(42))
    defaultStringifier(expr, varFor("node"), Seq("prop")) should equal("node.prop = 42")
  }

  test("should stringify ManyQueryExpression with entity as OR") {
    val expr = ManyQueryExpression(listOf(literalInt(1), literalInt(2)))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop = 1 OR 2")
  }

  test("should stringify ManyQueryExpression with entity as IN") {
    val expr = ManyQueryExpression(parameter("list", CTAny))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop IN $list")
  }

  test("should stringify ExistenceQueryExpression with entity") {
    val expr = ExistenceQueryExpression
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop")
  }

  test("should stringify RangeQueryExpression STARTS WITH with entity") {
    val expr = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(literalString("abc")))(pos))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop STARTS WITH \"abc\"")
  }

  test("should stringify RangeQueryExpression > with entity") {
    val range = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop > 10")
  }

  test("should stringify RangeQueryExpression between with entity") {
    val gt = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(10))))
    val lt = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(20))))
    val range = RangeBetween(gt, lt)
    val expr = RangeQueryExpression(InequalitySeekRangeWrapper(range)(pos))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("10 < n.prop < 20")
  }

  test("should stringify CompositeQueryExpression with entity") {
    val expr = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      SingleQueryExpression(literalString("abc"))
    ))
    defaultStringifier(expr, varFor("n"), Seq("prop1", "prop2")) should equal("n.prop1 = 1, n.prop2 = \"abc\"")
  }

  test("should stringify with entity that needs backticking") {
    val expr = SingleQueryExpression(literalInt(42))
    defaultStringifier(expr, varFor("my node"), Seq("prop")) should equal("`my node`.prop = 42")
  }

  // PointBoundingBoxSeekRangeWrapper tests
  test("should stringify PointBoundingBoxSeekRangeWrapper without entity") {
    val expr = RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
      PointBoundingBoxRange(parameter("lowerLeft", CTAny), parameter("upperRight", CTAny))
    )(pos))
    defaultStringifier(expr, Seq("prop")) should equal("point.withinBBox(prop, $lowerLeft, $upperRight)")
  }

  test("should stringify PointBoundingBoxSeekRangeWrapper with entity") {
    val expr = RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
      PointBoundingBoxRange(parameter("lowerLeft", CTAny), parameter("upperRight", CTAny))
    )(pos))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("point.withinBBox(n.prop, $lowerLeft, $upperRight)")
  }

  // PointDistanceSeekRangeWrapper tests
  test("should stringify PointDistanceSeekRangeWrapper inclusive without entity") {
    val expr = RangeQueryExpression(PointDistanceSeekRangeWrapper(
      PointDistanceRange(parameter("myPoint", CTAny), parameter("myDistance", CTAny), inclusive = true)
    )(pos))
    defaultStringifier(expr, Seq("prop")) should equal("point.distance(prop, $myPoint) <= $myDistance")
  }

  test("should stringify PointDistanceSeekRangeWrapper exclusive without entity") {
    val expr = RangeQueryExpression(PointDistanceSeekRangeWrapper(
      PointDistanceRange(parameter("myPoint", CTAny), parameter("myDistance", CTAny), inclusive = false)
    )(pos))
    defaultStringifier(expr, Seq("prop")) should equal("point.distance(prop, $myPoint) < $myDistance")
  }

  test("should stringify PointDistanceSeekRangeWrapper with entity") {
    val expr = RangeQueryExpression(PointDistanceSeekRangeWrapper(
      PointDistanceRange(parameter("myPoint", CTAny), parameter("myDistance", CTAny), inclusive = true)
    )(pos))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("point.distance(n.prop, $myPoint) <= $myDistance")
  }

  // AllQueryExpression and NonExistenceQueryExpression tests
  test("should stringify AllQueryExpression standalone and in composite") {
    defaultStringifier(AllQueryExpression, Seq("prop")) should equal("prop")
    val composite = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      AllQueryExpression
    ))
    defaultStringifier(composite, Seq("prop1", "prop2")) should equal("prop1 = 1, prop2")
  }

  test("should stringify NonExistenceQueryExpression standalone and in composite") {
    defaultStringifier(NonExistenceQueryExpression, Seq("prop")) should equal("NOT prop")
    val composite = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      NonExistenceQueryExpression
    ))
    defaultStringifier(composite, Seq("prop1", "prop2")) should equal("prop1 = 1, NOT prop2")
  }

  test("should stringify CompositeQueryExpression with AND separator") {
    val expr = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      SingleQueryExpression(literalString("abc"))
    ))
    cypherStringifier(expr, Seq("prop1", "prop2")) should equal("prop1 = 1 AND prop2 = \"abc\"")
  }

  test("should stringify ExistenceQueryExpression with IsNotNull form") {
    cypherStringifier(ExistenceQueryExpression, Seq("prop")) should equal("prop IS NOT NULL")
  }

  test("should stringify ExistenceQueryExpression with IsNotNull form and entity") {
    cypherStringifier(ExistenceQueryExpression, varFor("n"), Seq("prop")) should equal("n.prop IS NOT NULL")
  }

  test("should stringify AllQueryExpression with IsNotNull form") {
    cypherStringifier(AllQueryExpression, Seq("prop")) should equal("true")
  }

  test("should stringify NonExistenceQueryExpression with IsNotNull form") {
    cypherStringifier(NonExistenceQueryExpression, Seq("prop")) should equal("prop IS NULL")
  }

  test("should stringify NonExistenceQueryExpression with IsNotNull form and entity") {
    cypherStringifier(NonExistenceQueryExpression, varFor("n"), Seq("prop")) should equal("n.prop IS NULL")
  }

  test("should stringify CompositeQueryExpression with IsNotNull form and AND separator") {
    val composite = CompositeQueryExpression(Seq(
      SingleQueryExpression(literalInt(1)),
      ExistenceQueryExpression,
      NonExistenceQueryExpression
    ))
    cypherStringifier(composite, Seq("prop1", "prop2", "prop3")) should equal(
      "prop1 = 1 AND prop2 IS NOT NULL AND prop3 IS NULL"
    )
  }

  test("should backtick property names with special characters under IsNotNull form") {
    cypherStringifier(ExistenceQueryExpression, Seq("prop with spaces")) should equal(
      "`prop with spaces` IS NOT NULL"
    )
    cypherStringifier(NonExistenceQueryExpression, varFor("n"), Seq("prop with spaces")) should equal(
      "n.`prop with spaces` IS NULL"
    )
  }

  // Property name backticking tests
  test("should backtick property names with special characters") {
    val expr = SingleQueryExpression(literalInt(42))
    defaultStringifier(expr, Seq("prop")) should equal("prop = 42")
    defaultStringifier(expr, Seq("prop with spaces")) should equal("`prop with spaces` = 42")
  }

  test("should backtick property names with entity") {
    val expr = SingleQueryExpression(literalInt(42))
    defaultStringifier(expr, varFor("n"), Seq("prop")) should equal("n.prop = 42")
    defaultStringifier(expr, varFor("n"), Seq("prop with spaces")) should equal("n.`prop with spaces` = 42")
  }

  // Unhandled range expression test
  test("should throw for unhandled RangeQueryExpression") {
    val expr = RangeQueryExpression(literalInt(42))
    an[IllegalStateException] should be thrownBy defaultStringifier(expr, Seq("prop"))
  }
}
