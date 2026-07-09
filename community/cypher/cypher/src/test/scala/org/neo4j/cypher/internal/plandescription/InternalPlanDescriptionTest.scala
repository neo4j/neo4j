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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.CypherPlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.Distinctness
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersionArgument
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.query.statistic.PlanDetailsToBeLogged

import scala.jdk.CollectionConverters.MapHasAsScala

class InternalPlanDescriptionTest extends CommunityCypherTestSuite {

  private val ID = Id(0)

  test("arguments are read as expected") {
    val arguments = Seq(
      DbHits(1),
      PageCacheHits(2),
      PageCacheMisses(3),
      Time(4),
      Rows(5)
    )
    val plan = PlanDescriptionImpl(ID, "plan", Seq.empty, arguments, Set())
    plan.hasProfilerStatistics should equal(true)
    plan.getProfilerStatistics.getDbHits should equal(1)
    plan.getProfilerStatistics.getPageCacheHits should equal(2)
    plan.getProfilerStatistics.getPageCacheMisses should equal(3)
    plan.getProfilerStatistics.getTime should equal(4)
    plan.getProfilerStatistics.getRows should equal(5)
  }

  test("flatten behaves like expected for plan with two children") {
    val child1 = PlanDescriptionImpl(ID, "child1", Seq.empty, Seq.empty, Set())
    val child2 = PlanDescriptionImpl(ID, "child2", Seq.empty, Seq.empty, Set())
    val top = PlanDescriptionImpl(ID, "top", Seq(child1, child2), Seq.empty, Set())

    top.flatten should equal(Seq(top, child1, child2))
  }

  test("single plan flattened stays single") {
    val single = PlanDescriptionImpl(ID, "single", Seq.empty, Seq.empty, Set())

    single.flatten should equal(Seq(single))
  }

  test("left leaning plan should also flatten out nicely") {
    val leaf = PlanDescriptionImpl(ID, "leaf", Seq.empty, Seq.empty, Set())
    val lvl1 = PlanDescriptionImpl(ID, "lvl1", Seq(leaf), Seq.empty, Set())
    val lvl2 = PlanDescriptionImpl(ID, "lvl2", Seq(lvl1), Seq.empty, Set())
    val root = PlanDescriptionImpl(ID, "root", Seq(lvl2), Seq.empty, Set())

    root.flatten should equal(Seq(root, lvl2, lvl1, leaf))
  }

  test("bushy tree flattens correctly") {
    /*
                  A
             B1      B2
           C1  C2  C3  C4
     */
    val C4 = PlanDescriptionImpl(ID, "C4", Seq.empty, Seq.empty, Set())
    val C3 = PlanDescriptionImpl(ID, "C3", Seq.empty, Seq.empty, Set())
    val C2 = PlanDescriptionImpl(ID, "C2", Seq.empty, Seq.empty, Set())
    val C1 = PlanDescriptionImpl(ID, "C1", Seq.empty, Seq.empty, Set())
    val B2 = PlanDescriptionImpl(ID, "B2", Seq(C3, C4), Seq.empty, Set())
    val B1 = PlanDescriptionImpl(ID, "B1", Seq(C1, C2), Seq.empty, Set())
    val A = PlanDescriptionImpl(ID, "A", Seq(B1, B2), Seq.empty, Set())

    A.flatten should equal(Seq(A, B1, C1, C2, B2, C3, C4))
  }

  test("toString should render nicely") {
    val version = "5.0"
    val planDescription = PlanDescriptionImpl(ID, "Leaf", Seq.empty, Seq.empty, Set())
      .addArgument(Version(version))
      .addArgument(Planner("COST"))
      .addArgument(RuntimeVersion(version))
      .addArgument(Runtime("PIPELINED"))
      .addArgument(BatchSize(128))
      // use the legacy version which does not show in the description
      .addArgument(PlannerVersionArgument.currentVersion)

    normalizeNewLines(planDescription.toString) should equal(
      normalizeNewLines(s"""Cypher $version
                           |
                           |Planner COST
                           |
                           |Runtime PIPELINED
                           |
                           |Runtime version $version
                           |
                           |Batch size 128
                           |
                           |+----------+----+
                           || Operator | Id |
                           |+----------+----+
                           || +Leaf    |  0 |
                           |+----------+----+
                           |
                           |Total database accesses: ?
                           |""".stripMargin)
    )
  }

  test("toString should render nicely with planner version") {
    val version = "5.0"
    val plannerVersion = "EXPERIMENTAL"
    val planDescription = PlanDescriptionImpl(ID, "Leaf", Seq.empty, Seq.empty, Set())
      .addArgument(Version(version))
      .addArgument(Planner("COST"))
      .addArgument(RuntimeVersion(version))
      .addArgument(Runtime("PIPELINED"))
      .addArgument(BatchSize(128))
      .addArgument(CypherPlannerVersion(plannerVersion))

    normalizeNewLines(planDescription.toString) should equal(
      normalizeNewLines(
        s"""Cypher $version
           |
           |Planner COST
           |
           |Planner version $plannerVersion
           |
           |Runtime PIPELINED
           |
           |Runtime version $version
           |
           |Batch size 128
           |
           |+----------+----+
           || Operator | Id |
           |+----------+----+
           || +Leaf    |  0 |
           |+----------+----+
           |
           |Total database accesses: ?
           |""".stripMargin
      )
    )
  }

  test("logInfo should include defaults and omit absent optional arguments for a single operator") {
    val planDescription = plan(0, "Leaf")

    val logInfo = planDescription.logInfo()
    val operators = operatorMaps(logInfo)

    operators should equal(Seq(Map(
      "operatorName" -> "Leaf",
      "operatorId" -> 0
    )))
  }

  test("logInfo should include existing plan and operator arguments") {
    val planDescription = plan(
      7,
      "NodeIndexSeek",
      arguments = Seq(
        Version("25"),
        CypherPlannerVersion("EXPERIMENTAL"),
        Runtime("PIPELINED"),
        BatchSize(128),
        Details(Seq(asPrettyString.raw("n:Label(prop)"), asPrettyString.raw("cache[n.prop]"))),
        EstimatedRows(13.37, Some(42.0)),
        Order(asPrettyString.raw("n.prop ASC")),
        Distinctness(asPrettyString.raw("n")),
        PipelineInfo(3, fused = true, markAsSerial = false)
      )
    )

    val logInfo = planDescription.logInfo()
    val operators = operatorMaps(logInfo)
    operators should have size 1
    val operator = operators.head

    operator("operatorName") should equal("NodeIndexSeek")
    operator("operatorId") should equal(7)
    operator("details") should equal("n:Label(prop), cache[n.prop]")
    operator("order") should equal("n.prop ASC")
    operator("distinctness") should equal("n")

    val estimatedRows = operator("estimatedRows").asInstanceOf[java.util.Map[String, java.lang.Double]].asScala.toMap
    estimatedRows should equal(Map(
      "effectiveCardinality" -> 13.37,
      "rawCardinality" -> 42.0
    ))

    val pipelineInfo = operator("pipelineInfo").asInstanceOf[java.util.Map[String, Object]].asScala.toMap
    pipelineInfo should equal(Map(
      "pipelineId" -> "3",
      "requiresSerialExecution" -> Boolean.box(false)
    ))
  }

  test("logInfo should include operators with left and right children in nested trees") {
    /*
     *           Root(0)
     *          /        \
     *      Left(4)     Right(1)
     *        |          /      \
     *   LeftLeaf(5) RightLeft(3) RightRight(2)
     */
    val leftLeaf = plan(5, "LeftLeaf")
    val left = plan(4, "Left", Seq(leftLeaf))
    val rightLeft = plan(3, "RightLeft")
    val rightRight = plan(2, "RightRight")
    val right = plan(1, "Right", Seq(rightLeft, rightRight))
    val root = plan(0, "Root", Seq(left, right))

    val operators = operatorMaps(root.logInfo())

    operators.map(_("operatorName")) should equal(Seq("Root", "Right", "RightRight", "RightLeft", "Left", "LeftLeaf"))
    operators.map(_("operatorId")) should equal(Seq(0, 1, 2, 3, 4, 5))
    operators.foreach { operator =>
      operator("operatorName") shouldBe a[String]
      operator("operatorId") shouldBe a[Integer]
    }

    operatorByName(operators, "Root") should contain allOf (
      "leftChildOperatorId" -> 4,
      "rightChildOperatorId" -> 1
    )
    operatorByName(operators, "Left") should contain("leftChildOperatorId" -> 5)
    operatorByName(operators, "Left") should not contain key("rightChildOperatorId")
    operatorByName(operators, "Right") should contain allOf (
      "leftChildOperatorId" -> 3,
      "rightChildOperatorId" -> 2
    )
    operatorByName(operators, "LeftLeaf") should not contain key("leftChildOperatorId")
    operatorByName(operators, "LeftLeaf") should not contain key("rightChildOperatorId")
    operatorByName(operators, "RightLeft") should not contain key("leftChildOperatorId")
    operatorByName(operators, "RightRight") should not contain key("rightChildOperatorId")
  }

  private def plan(
    id: Int,
    name: String,
    children: Seq[InternalPlanDescription] = Seq.empty,
    arguments: Seq[Argument] = Seq.empty
  ): InternalPlanDescription = PlanDescriptionImpl(Id(id), name, children, arguments, Set())

  private def operatorMaps(logInfo: PlanDetailsToBeLogged): Seq[Map[String, Object]] =
    logInfo.getOperatorDetails.toSeq.map(_.toMap.asScala.toMap)

  private def operatorByName(operators: Seq[Map[String, Object]], name: String): Map[String, Object] =
    operators.find(_("operatorName") == name).getOrElse(fail(s"Expected operator '$name' in $operators"))
}
