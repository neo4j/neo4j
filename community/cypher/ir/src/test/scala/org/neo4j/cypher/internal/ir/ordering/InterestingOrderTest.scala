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
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.NoSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InterestingOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should reverse project property to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo")))
    // projection
    val projections = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), projections)))
  }

  test("should reverse project property to variable (descending)") {
    val io = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo")))
    // projection
    val projections = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projections)))
  }

  test("should reverse project property to property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo")))
    // projection
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo"), projections)))
  }

  test("should reverse project variable to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projection
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projections)))
  }

  test("cannot reverse project expression") {
    val expr: Expression = Add(varFor("xfoo"), prop("y", "foo"))(pos)
    // projection
    val projection = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
    val io = InterestingOrder.required(RequiredOrderCandidate.desc(expr, projection))

    // when applying the projection on the interestingOrder
    val result = io.withReverseProjectedColumns(Map.empty, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.empty))
  }

  test("should reverse project variable to variable and then to another variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projections
    val projection1 = Map[LogicalVariable, Expression](v"y" -> varFor("x"))
    val projection2 = Map[LogicalVariable, Expression](v"x" -> varFor("z"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"), projection2)))
  }

  test("should reverse project variable and property in multiple steps") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo")))
    // projections ... WITH z AS y ... WITH y AS x ... WITH x.foo AS xfoo ORDER BY xfoo
    val projection1 = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
    val projection2 = Map[LogicalVariable, Expression](v"x" -> varFor("y"))
    val projection3 = Map[LogicalVariable, Expression](v"y" -> varFor("z"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo"), projection2)))

    // and when
    val result3 = result2.withReverseProjectedColumns(projection3, Set.empty)

    // then
    result3 should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo"), projection3)))
  }

  test("should apply old projections") {
    val projection1 = Map[LogicalVariable, Expression](v"z" -> varFor("y"))
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("z"), projection1))
    val projection2 = Map[LogicalVariable, Expression](v"y" -> varFor("x"))

    val result = io.withReverseProjectedColumns(projection2, Set.empty)
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projection2)))
  }

  test("should only keep relevant projections") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projection
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"), v"bar" -> varFor("baz"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), Map(v"y" -> varFor("x")))))
  }

  test("should not reverse project variable to variable if not argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(
      Map.empty,
      Set.empty
    )
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(
      Map.empty,
      Set(v"y")
    )
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))))
  }

  test("should reverse project property to property if variable is argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "prop"))).withReverseProjectedColumns(
      Map.empty,
      Set(v"y")
    )
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "prop"))))
  }

  test("should reverse project variable to variable if is argument when projections exist") {
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(
      projections,
      Set(v"y")
    )
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projections)))
  }

  test("should reverse project property to property if variable is argument when projections exist") {
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "prop"))).withReverseProjectedColumns(
      projections,
      Set(v"y")
    )
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "prop"), projections)))
  }

  test("should reverse project variable to variable if is argument when old projections exist") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), Map(v"y" -> varFor("x"))))
    val result = io.withReverseProjectedColumns(Map.empty, Set(v"x"))
    result should be(io)
  }

  test("should reverse project property to property if variable is argument when old projections exist") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "prop"), Map(v"y" -> varFor("x"))))
    val result = io.withReverseProjectedColumns(Map.empty, Set(v"x"))
    result should be(io)
  }

  test("Required order is not satisfied by an empty provided order") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))

    io.satisfiedBy(ProvidedOrder.empty) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order is not satisfied by provided order in other direction") {
    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")))

    val poAsc = ProvidedOrder.asc(prop("x", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    ioAsc.satisfiedBy(poDesc) should matchPattern { case NoSatisfaction() => }
    ioDesc.satisfiedBy(poAsc) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order should be satisfied by provided order on same property") {
    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")))

    val poAsc = ProvidedOrder.asc(prop("x", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    ioAsc.satisfiedBy(poAsc) should matchPattern { case FullSatisfaction() => }
    ioDesc.satisfiedBy(poDesc) should matchPattern { case FullSatisfaction() => }
  }

  test("Required order should be satisfied by provided order on same renamed property") {
    val projectionSeveralSteps = Map[LogicalVariable, Expression](v"x" -> varFor("y"), v"y" -> varFor("z"))
    val projectionOneStep = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo"), projectionSeveralSteps))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projectionOneStep))

    val poAsc = ProvidedOrder.asc(prop("z", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    ioAsc.satisfiedBy(poAsc) should matchPattern { case FullSatisfaction() => }
    ioDesc.satisfiedBy(poDesc) should matchPattern { case FullSatisfaction() => }
  }

  test("Required order should not be satisfied by provided order on different property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val po = ProvidedOrder.asc(prop("y", "foo"))

    io.satisfiedBy(po) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order on renamed property should not be satisfied by provided order on different property") {
    val projection = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    val io = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projection))
    val po = ProvidedOrder.desc(prop("y", "foo"))

    io.satisfiedBy(po) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order on expression is not satisfied by provided order on property") {
    val projection =
      Map[LogicalVariable, Expression](v"add" -> Add(prop("x", "foo"), SignedDecimalIntegerLiteral("42")(pos))(pos))

    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("add"), projection))
    val po = ProvidedOrder.asc(prop("x", "foo"))

    io.satisfiedBy(po) should matchPattern { case NoSatisfaction() => }
  }

  test("should transform required to interesting") {
    val projection = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
    val ro = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), projection))
    val io = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), projection))

    ro.asInteresting should be(io)
    io.asInteresting should be(io)
  }

  // test the interesting part of the InterestingOrder

  test("should reverse project property to variable (for both)") {
    val projection = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"), v"yfoo" -> prop("y", "foo"))

    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"))).interesting(
      InterestingOrderCandidate.desc(varFor("yfoo"))
    )

    val result = io.withReverseProjectedColumns(projection, Set.empty)
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(
      varFor("xfoo"),
      Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
    ))
      .interesting(InterestingOrderCandidate.desc(
        varFor("yfoo"),
        Map[LogicalVariable, Expression](v"yfoo" -> prop("y", "foo"))
      )))
  }

  test("should reverse project property to variable (for interesting)") {
    // projection
    val projections = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    val ioAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo")))
    val ioDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo")))

    // when
    val resultAsc = ioAsc.withReverseProjectedColumns(projections, Set.empty)
    val resultDesc = ioDesc.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultAsc should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), projections)))
    resultDesc should be(InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo"), projections)))
  }

  test("should reverse project property to property and variable to variable (for interesting)") {
    // projection
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))

    val ioProp = InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo")))
    val ioVar = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y")))

    // when
    val resultProp = ioProp.withReverseProjectedColumns(projections, Set.empty)
    val resultVar = ioVar.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultProp should be(InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo"), projections)))
    resultVar should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"), projections)))
  }

  test("should filter out empty interesting orders when reverse projecting") {
    // projection
    val projections = Map[LogicalVariable, Expression](v"y" -> varFor("x"))

    val io = InterestingOrder.interested(InterestingOrderCandidate.empty)
      .interesting(InterestingOrderCandidate.asc(prop("y", "foo"))).interesting(InterestingOrderCandidate.empty)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo"), projections)))
  }

  test("should reverse project variable to variable and then to another variable (for interesting)") {
    val io = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y")))
    // projections ... WITH x.foo AS xfoo ... WITH xfoo AS y
    val projection1 = Map[LogicalVariable, Expression](v"y" -> varFor("xfoo"))
    val projection2 = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), projection2)))
  }

  test("should not reverse project variable to variable if not argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(
      Map.empty,
      Set.empty
    )
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(
      Map.empty,
      Set(v"y")
    )
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))))
  }

  // Test partial satisfaction

  test("Empty required order satisfied by anything") {
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.empty) should matchPattern { case FullSatisfaction() => }
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc(varFor("x"))) should matchPattern { case FullSatisfaction() =>
    }
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc(varFor("x"))) should matchPattern { case FullSatisfaction() =>
    }
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y"))) should matchPattern {
      case FullSatisfaction() =>
    }
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc(varFor("x")).desc(varFor("y"))) should matchPattern {
      case FullSatisfaction() =>
    }
  }

  test("Single property required order satisfied by matching provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.asc(varFor("x"))
    ) should matchPattern { case FullSatisfaction() => }
  }

  test("Single property required order satisfied by longer provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.asc(varFor("x")).asc(varFor("y"))
    ) should matchPattern { case FullSatisfaction() => }
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y"))
    ) should matchPattern { case FullSatisfaction() => }
  }

  test("Single property required order not satisfied by mismatching provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.asc(varFor("y"))
    ) should matchPattern { case NoSatisfaction() => }
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.desc(varFor("x"))
    ) should matchPattern { case NoSatisfaction() => }
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(
      ProvidedOrder.asc(varFor("y")).asc(varFor("x"))
    ) should matchPattern { case NoSatisfaction() => }
  }

  test("Multi property required order can yield partial satisfaction") {
    val interestingOrder =
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")))

    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x"))) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.desc(varFor("x"))) should matchPattern { case NoSatisfaction() => }
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("y"))) should matchPattern { case NoSatisfaction() => }

    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y"))) should be(Satisfaction(
      Seq(Asc(varFor("x")), Desc(varFor("y"))),
      Seq(Asc(varFor("z")))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y"))) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("z"))) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.desc(varFor("x")).desc(varFor("y"))) should matchPattern {
      case NoSatisfaction() =>
    }

    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case FullSatisfaction() => }
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("z")).asc(varFor("y"))) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y")).desc(varFor("z"))) should be(
      Satisfaction(Seq(Asc(varFor("x")), Desc(varFor("y"))), Seq(Asc(varFor("z"))))
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y")).desc(varFor("z"))) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y")).asc(varFor("z"))) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    interestingOrder.satisfiedBy(
      ProvidedOrder.desc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case NoSatisfaction() => }

    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).asc(varFor("a"))
    ) should matchPattern { case FullSatisfaction() => }
    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("a")).asc(varFor("z"))
    ) should be(Satisfaction(Seq(Asc(varFor("x")), Desc(varFor("y"))), Seq(Asc(varFor("z")))))
    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("a")).asc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case NoSatisfaction() => }
  }

  test("Multi property required order (with projections) can yield partial satisfaction") {
    val projection = Map[LogicalVariable, Expression](v"newX" -> varFor("x"))
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("newX"), projection).desc(
      varFor("y"),
      projection
    ).asc(varFor("z"), projection))

    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x"))) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.desc(varFor("x"))) should matchPattern { case NoSatisfaction() => }
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("y"))) should matchPattern { case NoSatisfaction() => }

    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y"))) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
      Seq(Asc(varFor("z"), projection))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y"))) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("z"))) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    interestingOrder.satisfiedBy(ProvidedOrder.desc(varFor("x")).desc(varFor("y"))) should matchPattern {
      case NoSatisfaction() =>
    }

    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case FullSatisfaction() => }
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("z")).asc(varFor("y"))) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y")).desc(varFor("z"))) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
        Seq(Asc(varFor("z"), projection))
      )
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y")).desc(varFor("z"))) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y")).asc(varFor("z"))) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    interestingOrder.satisfiedBy(
      ProvidedOrder.desc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case NoSatisfaction() => }

    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).asc(varFor("a"))
    ) should matchPattern { case FullSatisfaction() => }
    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("a")).asc(varFor("z"))
    ) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
      Seq(Asc(varFor("z"), projection))
    ))
    interestingOrder.satisfiedBy(
      ProvidedOrder.asc(varFor("a")).asc(varFor("x")).desc(varFor("y")).asc(varFor("z"))
    ) should matchPattern { case NoSatisfaction() => }
  }

  // forQueryGraph

  test("forQueryGraph should keep only the column with dependencies met") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a")).asc(varFor("b")))

    io.forQueryGraph(QueryGraph(patternNodes = Set(v"a"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a")))
    )
    io.forQueryGraph(QueryGraph(patternNodes = Set(v"b"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("b")))
    )
  }

  test("forQueryGraph should be empty if no column with dependencies met") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a")).asc(varFor("b")))

    io.forQueryGraph(QueryGraph(patternNodes = Set(v"c"))) should equal(
      InterestingOrder.empty
    )
  }

  test("forQueryGraph should keep only the column with dependencies met, with no gaps in columns") {
    val io = InterestingOrder.required(RequiredOrderCandidate
      .asc(varFor("a"))
      .asc(varFor("b"))
      .asc(varFor("c"))
      .asc(varFor("d")))

    io.forQueryGraph(QueryGraph(patternNodes = Set(v"a", v"b"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a")).asc(varFor("b")))
    )
    io.forQueryGraph(QueryGraph(patternNodes = Set(v"a", v"c"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a")))
    )
    io.forQueryGraph(QueryGraph(patternNodes = Set(v"b", v"c"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("b")).asc(varFor("c")))
    )
    io.forQueryGraph(QueryGraph(patternNodes = Set(v"b", v"d"))) should equal(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("b")))
    )
  }

}
