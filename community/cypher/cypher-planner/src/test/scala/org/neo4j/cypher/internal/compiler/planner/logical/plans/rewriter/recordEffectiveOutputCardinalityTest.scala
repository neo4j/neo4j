/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.Batched
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class recordEffectiveOutputCardinalityTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  private val precision = 0.00001
  private val leafCardinality = 10

  private def noAttributes(idGen: IdGen) = Attributes[LogicalPlan](idGen)

  test("Should update effective cardinality through cartesian product") {
    // GIVEN
    val limit = 5

    val initialPlan = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(limit)
      .limit(limit).withCardinality(limit)
      .cartesianProduct().withCardinality(leafCardinality * leafCardinality)
      .|.allNodeScan("m").withCardinality(leafCardinality)
      .allNodeScan("n").withCardinality(leafCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(limit)
      .limit(limit).withCardinality(limit)
      .cartesianProduct().withCardinality(limit)
      .|.allNodeScan("m").withCardinality(Math.sqrt(limit))
      .allNodeScan("n").withCardinality(Math.sqrt(limit))

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should favour lower limit") {
    // GIVEN
    val lowLimit = 2
    val highLimit = 8

    val initialPlan = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(lowLimit)
      .limit(lowLimit).withCardinality(lowLimit)
      .limit(highLimit).withCardinality(highLimit)
      .cartesianProduct().withCardinality(leafCardinality * leafCardinality)
      .|.allNodeScan("m").withCardinality(leafCardinality)
      .allNodeScan("n").withCardinality(leafCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(lowLimit)
      .limit(lowLimit).withCardinality(lowLimit)
      .limit(highLimit).withCardinality(lowLimit)
      .cartesianProduct().withCardinality(lowLimit)
      .|.allNodeScan("m").withCardinality(Math.sqrt(lowLimit))
      .allNodeScan("n").withCardinality(Math.sqrt(lowLimit))

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should update HashJoin correctly") {
    val limit = 1
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(limit)
      .limit(limit)
      .nodeHashJoin("n").withCardinality(leafCardinality)
      .|.allNodeScan("n").withCardinality(leafCardinality)
      .allNodeScan("m").withCardinality(leafCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(limit)
      .limit(limit).withCardinality(limit)
      .nodeHashJoin("n").withCardinality(limit)
      .|.allNodeScan("n").withCardinality(limit)
      .allNodeScan("m").withCardinality(leafCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Updates under ExhaustiveLimit should have full cardinality") {
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(10)
      .exhaustiveLimit(10).withCardinality(10)
      .create(createNode("n", "N")).withCardinality(100)
      .argument()

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(10)
      .exhaustiveLimit(10).withCardinality(10)
      .create(createNode("n", "N")).withCardinality(100)
      .argument()

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS of cartesian product") {
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("o")
      .cartesianProduct().withCardinality(100)
      .|.projection("n as `n`").withCardinality(10)
      .|.allNodeScan("n").withCardinality(10)
      .allNodeScan("o").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan, ExecutionModel.Volcano)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("o")
      .cartesianProduct().withCardinality(100)
      .|.projection("n as `n`").withCardinality(100)
      .|.allNodeScan("n").withCardinality(100)
      .allNodeScan("o").withCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiple RHSs of nested cartesian product in slotted") {
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(100)
      .|.|.projection("m AS `m`").withCardinality(10)
      .|.|.allNodeScan("m").withCardinality(10)
      .|.allNodeScan("n").withCardinality(10)
      .allNodeScan("o").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan, ExecutionModel.Volcano)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(1000)
      .|.|.projection("m AS `m`").withCardinality(1000)
      .|.|.allNodeScan("m").withCardinality(1000)
      .|.allNodeScan("n").withCardinality(100)
      .allNodeScan("o").withCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiple RHSs of nested cartesian product for batched execution") {
    val batchSize = 4
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(100)
      .|.|.projection("m AS `m`").withCardinality(10)
      .|.|.allNodeScan("m").withCardinality(10)
      .|.allNodeScan("n").withCardinality(10)
      .allNodeScan("o").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan, Batched(batchSize, batchSize))

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(300)
      .|.|.projection("m AS `m`").withCardinality(90)
      .|.|.allNodeScan("m").withCardinality(90)
      .|.allNodeScan("n").withCardinality(30)
      .allNodeScan("o").withCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiple RHSs of nested cartesian product under limit for batched execution") {
    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("m").withCardinality(1000)
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(100)
      .|.|.projection("m AS `m`").withCardinality(10)
      .|.|.allNodeScan("m").withCardinality(10)
      .|.allNodeScan("n").withCardinality(10)
      .allNodeScan("o").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan, Volcano)

    // THEN

    val expected = new LogicalPlanBuilder()
      .produceResults("m").withCardinality(1000)
      .cartesianProduct().withCardinality(1000)
      .|.cartesianProduct().withCardinality(1000)
      .|.|.projection("m AS `m`").withCardinality(1000)
      .|.|.allNodeScan("m").withCardinality(1000)
      .|.allNodeScan("n").withCardinality(100)
      .allNodeScan("o").withCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.cardinalities

    (plan, cardinalities).should(haveSameCardinalitiesAs((expectedPlan, expectedCards)))
  }

  def haveSameCardinalitiesAs(expected: (LogicalPlan, Cardinalities)): Matcher[(LogicalPlan, Cardinalities)] =
    (actual: (LogicalPlan, Cardinalities)) => {
      val (actPlan, actCards) = actual
      val (expPlan, expCards) = expected
      val planPairs = actPlan.flatten.zip(expPlan.flatten)

      val planMismatch = planPairs.collectFirst {
        case (act, exp) if act != exp =>
          val actPlanString = LogicalPlanToPlanBuilderString(act)
          val expPlanString = LogicalPlanToPlanBuilderString(exp)
          MatchResult(
            matches = false,
            rawFailureMessage = s"Expected same plan structure but actual contained:\n$actPlanString\nand expected contained:\n$expPlanString",
            rawNegatedFailureMessage = "")
      }

      val results = planPairs.map {
        case (act, exp) =>
          val actCard = actCards(act.id)
          val expCard = expCards(exp.id)
          val actPlanString = LogicalPlanToPlanBuilderString(act)
          MatchResult(
            matches = (actCard.amount - expCard.amount).abs < precision,
            rawFailureMessage = s"Expected cardinality $expCard but was $actCard for plan:\n$actPlanString",
            rawNegatedFailureMessage = "")
      }

      val cardinalityMismatch = results.find(!_.matches)
      val ok = MatchResult(
        matches = true,
        rawFailureMessage = "",
        rawNegatedFailureMessage = "")

      (planMismatch orElse cardinalityMismatch) getOrElse ok
    }

  private def rewrite(pb: LogicalPlanBuilder, executionModel: ExecutionModel = ExecutionModel.default): (LogicalPlan, EffectiveCardinalities) = {
    val plan = pb.build().endoRewrite(recordEffectiveOutputCardinality(executionModel, pb.cardinalities, pb.effectiveCardinalities, noAttributes(pb.idGen)))
    (plan, pb.effectiveCardinalities)
  }
}
