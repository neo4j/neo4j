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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.physicalplanning.LivenessAnalysisTest.IncorrectLivenessException
import org.neo4j.cypher.internal.physicalplanning.LivenessAnalysisTest.PlanWithLiveAsserts
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.LiveVariables
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LivenessAnalysisTest extends CypherFunSuite {

  test("simple eager") {
    new PlanWithLiveAsserts()
      .produceResults("a", "c")
      .eager().expectLive("a", "c")
      .projection("a+b AS c")
      .projection("1 AS a", "2 AS b")
      .argument()
      .assertCorrectLiveness()
  }

  test("simple sort") {
    new PlanWithLiveAsserts()
      .produceResults("a")
      .sort("c ASC").expectLive("a", "c")
      .projection("a+b AS c")
      .projection("1 AS a", "2 AS b")
      .argument()
      .assertCorrectLiveness()
  }

  test("eager with apply") {
    new PlanWithLiveAsserts()
      .produceResults("a", "b")
      .eager().expectLive("a", "b")
      .apply()
      .|.eager().expectLive("a")
      .|.argument("a")
      .eager().expectLive("a", "b")
      .projection("1 AS a", "2 AS b", "3 AS c")
      .argument()
      .assertCorrectLiveness()
  }

  test("eager with nested applies") {
    new PlanWithLiveAsserts()
      .produceResults("a")
      .eager().expectLive("a")
      .apply()
      .|.filter("b > 0")
      .|.apply()
      .|.|.eager().expectLive("a", "b")
      .|.|.argument("a", "b")
      .|.eager().expectLive("a", "b")
      .|.argument("a", "b", "c")
      .projection("1 AS a", "2 AS b", "3 AS c")
      .argument()
      .assertCorrectLiveness()
  }

  test("eager with nested applies 2") {
    new PlanWithLiveAsserts()
      .produceResults("a")
      .eager().expectLive("a")
      .apply()
      .|.filter("c > 0")
      .|.eager().expectLive("a", "c")
      .|.apply()
      .|.|.filter("d > 0")
      .|.|.eager().expectLive("a", "c", "d")
      .|.|.apply()
      .|.|.|.eager().expectLive("a", "c", "d")
      .|.|.|.argument("a", "b", "c", "d")
      .|.|.eager().expectLive("a", "b", "c", "d")
      .|.|.argument("a", "b", "c", "d", "e")
      .|.eager().expectLive("a", "b", "c", "d", "e")
      .|.argument("a", "b", "c", "d", "e", "f")
      .eager().expectLive("a", "b", "c", "d", "e", "f")
      .projection("i+1 AS a", "i+2 AS b", "i+3 AS c", "i+4 AS d", "i+5 AS e", "i+6 AS f", "i+7 AS g")
      .unwind(s"range(0,$size) AS i")
      .argument()
      .assertCorrectLiveness()
  }

  test("eager on all sides of apply") {
    new PlanWithLiveAsserts()
      .produceResults("readResult1", "readResult2", "readResult3", "readResult4")
      .eager().expectLive("readResult1", "readResult2", "readResult3", "readResult4")
      .projection(
        "readTopOfApply1 + readTopOfApply2 + readTopOfApply3 AS readResult4",
        "10 AS neverRead4"
      )
      .apply()
      .|.projection(
        "8 AS neverRead3",
        "9 AS readResult3",
        "readApplyRhs1b + readApplyRhs2 AS readTopOfApply3"
      )
      .|.eager().expectLive(
        "readResult1",
        "readResult2",
        "readApplyRhs1b",
        "readApplyRhs2",
        "readTopOfApply1",
        "readTopOfApply2"
      )
      .|.projection(
        "readApplyRhs1a + readApplyRhs1b AS readApplyRhs2",
        "7 AS readResult2",
        "8 AS readTopOfApply2",
        "9 AS neverRead2"
      )
      .|.argument("readResult1", "readApplyRhs1a", "readApplyRhs1b", "readTopOfApply1")
      .eager().expectLive("readApplyRhs1a", "readApplyRhs1b", "readResult1", "readTopOfApply1")
      .projection(
        "1 AS neverRead1",
        "3 AS readApplyRhs1a",
        "4 AS readApplyRhs1b",
        "5 AS readTopOfApply1",
        "6 AS readResult1"
      )
      .argument()
      .assertCorrectLiveness()
  }

  test("eager with semi apply") {
    new PlanWithLiveAsserts()
      .produceResults("a", "b")
      .eager().expectLive("a", "b")
      .semiApply()
      .|.eager().expectLive("a")
      .|.argument("a")
      .eager().expectLive("a", "b")
      .projection("1 AS a", "2 AS b", "3 AS c")
      .argument()
      .assertCorrectLiveness()
  }

  test("procedure call") {
    val resolver = new LogicalPlanResolver(procedures =
      Set(ProcedureSignature(
        QualifiedName(Seq("test"), "proc"),
        IndexedSeq(FieldSignature("input", AnyType.instance)),
        Some(IndexedSeq(FieldSignature("output", AnyType.instance))),
        None,
        ProcedureReadOnlyAccess,
        id = 0
      ))
    )
    new PlanWithLiveAsserts(resolver)
      .produceResults("output")
      .procedureCall("test.proc(b)")
      .eager().expectLive("b", "output")
      .procedureCall("test.proc(a)")
      .eager().expectLive("a", "b")
      .projection("1 AS a", "2 AS b")
      .argument()
      .assertCorrectLiveness()
  }

  test("function invocation") {
    new PlanWithLiveAsserts()
      .produceResults("sizeA")
      .eager().expectLive("sizeA")
      .projection("size(ns) AS sizeA")
      .eager().expectLive("ns")
      .aggregation(Seq(), Seq("collect(n) AS ns"))
      .allNodeScan("n")
      .assertCorrectLiveness()
  }

  test("eager before apply that uses some live variables") {
    new PlanWithLiveAsserts()
      .produceResults("a")
      .apply()
      .|.filter("b > 0")
      .|.argument("b")
      .eager().expectLive("a", "b")
      .projection("1 AS a", "2 AS b", "3 AS c")
      .argument()
      .assertCorrectLiveness()
  }

  test("test works") {
    assertThrows[IncorrectLivenessException](
      new PlanWithLiveAsserts()
        .produceResults("a", "c")
        .eager().expectLive("a", "b", "c")
        .projection("a+b AS c")
        .projection("1 AS a", "2 AS b")
        .argument()
        .assertCorrectLiveness()
    )
    assertThrows[IncorrectLivenessException](
      new PlanWithLiveAsserts()
        .produceResults("a", "c")
        .eager().expectLive("a")
        .projection("a+b AS c")
        .projection("1 AS a", "2 AS b")
        .argument()
        .assertCorrectLiveness()
    )
  }

  test("projections with discard") {
    new PlanWithLiveAsserts()
      .produceResults("hi")
      .eager().expectLive("hi")
      .projection("keep as hi")
      .eager().expectLive("keep")
      .projection("keep as keep")
      .projection("'bla' + a as keep", "'blö' + a as discard")
      .unwind("range(0, 10) AS a")
      .argument()
      .assertCorrectLiveness()
  }

  test("nested plan collect expression") {
    new PlanWithLiveAsserts()
      .produceResults("x")
      .eager().expectLive("x")
      .nestedPlanCollectExpressionProjection("x", "b.prop")
      .|.eager()
      .|.expand("(a)-->(b)")
      .|.allNodeScan("a")
      .eager().expectLive()
      .projection("'i' AS i")
      .argument()
      .assertCorrectLiveness()
  }

  test("nested plan collect expression 2") {
    new PlanWithLiveAsserts()
      .produceResults("x")
      .eager().expectLive("x")
      .nestedPlanCollectExpressionProjection("x", "b.b")
      .|.eager()
      .|.projection("{b:1} AS b", "'x' AS c")
      .|.argument("a")
      .eager().expectLive("a")
      .nodeByLabelScan("a", "A")
      .assertCorrectLiveness()
  }

  test("nested plan exist expression") {
    new PlanWithLiveAsserts()
      .produceResults("r", "x")
      .eager().expectLive("r", "x")
      .nestedPlanExistsExpressionProjection("r")
      .|.eager()
      .|.expand("(a)-->(b)")
      .|.allNodeScan("a")
      .projection("1 AS x")
      .argument()
      .assertCorrectLiveness()
  }
}

object LivenessAnalysisTest {
  case class IncorrectLivenessException(msg: String) extends IllegalStateException(msg)

  class PlanWithLiveAsserts(resolver: Resolver = new LogicalPlanResolver)
      extends AbstractLogicalPlanBuilder[LogicalPlan, PlanWithLiveAsserts](resolver, wholePlan = true) {
    private val expectedLive = new LiveVariables

    def expectLive(variables: String*): PlanWithLiveAsserts = {
      expectedLive.set(idOfLastPlan, variables.toSet)
      this
    }

    def assertCorrectLiveness(): Unit = {
      val actual = LivenessAnalysis.computeLiveVariables(build())
      if (actual != expectedLive) {
        val maxId = (actual.iterator ++ expectedLive.iterator).map(_._1).maxBy(_.x)
        val diff = Range.inclusive(0, maxId.x)
          .map(Id.apply)
          .collect {
            case id if actual.getOption(id) != expectedLive.getOption(id) =>
              (id, actual.getOption(id), expectedLive.getOption(id))
          }
          .map { case (id, actualValue, expectedValue) =>
            s"$id: actual=${actualValue.map(_.toSeq.sorted)} expected=${expectedValue.map(_.toSeq.sorted)}"
          }

        throw IncorrectLivenessException(
          s"""Live variables diff:
             |${diff.mkString("\n")}""".stripMargin
        )
      }
    }

    override protected def build(readOnly: Boolean): LogicalPlan = buildLogicalPlan()
  }
}
