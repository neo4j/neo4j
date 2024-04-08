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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.RepetitionCardinalityModel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class recordEffectiveOutputCardinalityTest extends CypherFunSuite with LogicalPlanningAttributesTestSupport
    with AstConstructionTestSupport {
  private val leafCardinality = 10

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
    val (plan, cardinalities) = rewrite(initialPlan, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withEffectiveCardinality(limit)
      .limit(limit).withEffectiveCardinality(limit)
      .cartesianProduct().withEffectiveCardinality(limit)
      .|.allNodeScan("m").withEffectiveCardinality(limit)
      .allNodeScan("n").withEffectiveCardinality(1)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
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
    val (plan, cardinalities) = rewrite(initialPlan, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder()
      .produceResults("n").withEffectiveCardinality(lowLimit)
      .limit(lowLimit).withEffectiveCardinality(lowLimit)
      .limit(highLimit).withEffectiveCardinality(lowLimit)
      .cartesianProduct().withEffectiveCardinality(lowLimit)
      .|.allNodeScan("m").withEffectiveCardinality(lowLimit)
      .allNodeScan("n").withEffectiveCardinality(1)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
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
      .produceResults("n").withEffectiveCardinality(limit)
      .limit(limit).withEffectiveCardinality(limit)
      .nodeHashJoin("n").withEffectiveCardinality(limit)
      .|.allNodeScan("n").withEffectiveCardinality(limit)
      .allNodeScan("m").withEffectiveCardinality(leafCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
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
      .produceResults("n").withEffectiveCardinality(10)
      .exhaustiveLimit(10).withEffectiveCardinality(10)
      .create(createNode("n", "N")).withEffectiveCardinality(100)
      .argument()

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
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
      .cartesianProduct().withEffectiveCardinality(100)
      .|.projection("n as `n`").withEffectiveCardinality(100)
      .|.allNodeScan("n").withEffectiveCardinality(100)
      .allNodeScan("o").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
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
      .cartesianProduct().withEffectiveCardinality(1000)
      .|.cartesianProduct().withEffectiveCardinality(1000)
      .|.|.projection("m AS `m`").withEffectiveCardinality(1000)
      .|.|.allNodeScan("m").withEffectiveCardinality(1000)
      .|.allNodeScan("n").withEffectiveCardinality(100)
      .allNodeScan("o").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiple RHSs of nested cartesian product for batched execution") {
    val batchSize = 4
    val leafCardinality = 10
    val numBatchesLHSs = Math.ceil(leafCardinality / batchSize.toDouble)

    // GIVEN
    val initialPlan = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withCardinality(leafCardinality * leafCardinality * leafCardinality)
      .|.cartesianProduct().withCardinality(leafCardinality * leafCardinality)
      .|.|.projection("m AS `m`").withCardinality(leafCardinality)
      .|.|.allNodeScan("m").withCardinality(leafCardinality)
      .|.allNodeScan("n").withCardinality(leafCardinality)
      .allNodeScan("o").withCardinality(leafCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initialPlan, BatchedSingleThreaded(batchSize, batchSize))

    // THEN

    val expected = new LogicalPlanBuilder()
      .produceResults("m")
      .cartesianProduct().withEffectiveCardinality(leafCardinality * leafCardinality * leafCardinality)
      .|.cartesianProduct().withEffectiveCardinality(leafCardinality * leafCardinality * numBatchesLHSs)
      .|.|.projection("m AS `m`").withEffectiveCardinality(leafCardinality * numBatchesLHSs * numBatchesLHSs)
      .|.|.allNodeScan("m").withEffectiveCardinality(leafCardinality * numBatchesLHSs * numBatchesLHSs)
      .|.allNodeScan("n").withEffectiveCardinality(leafCardinality * numBatchesLHSs)
      .allNodeScan("o").withEffectiveCardinality(leafCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiple RHSs of nested cartesian product for volcano execution") {
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
      .produceResults("m").withEffectiveCardinality(1000)
      .cartesianProduct().withEffectiveCardinality(1000)
      .|.cartesianProduct().withEffectiveCardinality(1000)
      .|.|.projection("m AS `m`").withEffectiveCardinality(1000)
      .|.|.allNodeScan("m").withEffectiveCardinality(1000)
      .|.allNodeScan("n").withEffectiveCardinality(100)
      .allNodeScan("o").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of SemiApply, but also apply WorkReduction") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .semiApply().withCardinality(15)
      .|.expandAll("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .semiApply().withEffectiveCardinality(15)
      .|.expandAll("(n)-->(m)").withEffectiveCardinality(1 * 10)
      .|.allNodeScan("m").withEffectiveCardinality(5 * 10)
      .allNodeScan("n").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Apply") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .apply().withCardinality(20)
      .|.expandAll("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .apply().withEffectiveCardinality(20)
      .|.expandAll("(n)-->(m)").withEffectiveCardinality(2 * 10)
      .|.allNodeScan("m").withEffectiveCardinality(10 * 10)
      .allNodeScan("n").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Trail") {
    val lhsCardinality = 10
    val expandCardinality = 2
    val upperBound = 5

    // GIVEN
    val trailParameters = TrailParameters(
      min = 0,
      max = Limited(upperBound),
      start = "u",
      end = "v",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val initial = new LogicalPlanBuilder(false)
      .trail(trailParameters).withCardinality(20)
      .|.expandAll("(n)-[r]->(m)").withCardinality(expandCardinality)
      .|.argument("n").withCardinality(1)
      .allNodeScan("u").withCardinality(lhsCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    val expectedRHS = (0 until upperBound).map(Math.pow(expandCardinality, _)).sum * lhsCardinality
    // THEN
    val expected = new LogicalPlanBuilder(false)
      .trail(trailParameters).withEffectiveCardinality(20)
      .|.expandAll("(n)-[r]->(m)").withEffectiveCardinality(expandCardinality * expectedRHS)
      .|.argument("n").withEffectiveCardinality(expectedRHS)
      .allNodeScan("u").withEffectiveCardinality(lhsCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Trail with no upper bound") {
    val lhsCardinality = 10
    val expandCardinality = 2

    // GIVEN
    val trailParameters = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "v",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val initial = new LogicalPlanBuilder(false)
      .trail(trailParameters).withCardinality(20)
      .|.expandAll("(n)-[r]->(m)").withCardinality(expandCardinality)
      .|.argument("n").withCardinality(1)
      .allNodeScan("u").withCardinality(lhsCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    val expectedRHS =
      (0 until RepetitionCardinalityModel.MAX_VAR_LENGTH).map(Math.pow(expandCardinality, _)).sum * lhsCardinality
    // THEN
    val expected = new LogicalPlanBuilder(false)
      .trail(trailParameters).withEffectiveCardinality(20)
      .|.expandAll("(n)-[r]->(m)").withEffectiveCardinality(expandCardinality * expectedRHS)
      .|.argument("n").withEffectiveCardinality(expectedRHS)
      .allNodeScan("u").withEffectiveCardinality(lhsCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of nested instances of Apply") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .apply().withCardinality(60)
      .|.apply().withCardinality(6)
      .|.|.expandAll("(n)-->(m)").withCardinality(3)
      .|.|.allNodeScan("m").withCardinality(30)
      .|.expandAll("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(20)
      .allNodeScan("n").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .apply().withEffectiveCardinality(60)
      .|.apply().withEffectiveCardinality(6 * 10)
      .|.|.expandAll("(n)-->(m)").withEffectiveCardinality(3 * 2 * 10)
      .|.|.allNodeScan("m").withEffectiveCardinality(30 * 2 * 10)
      .|.expandAll("(n)-->(m)").withEffectiveCardinality(2 * 10)
      .|.allNodeScan("m").withEffectiveCardinality(20 * 10)
      .allNodeScan("n").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Apply and apply WorkReduction if there is a Limit") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(10).withCardinality(10)
      .apply().withCardinality(20)
      .|.expandAll("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(10).withEffectiveCardinality(10)
      .apply().withEffectiveCardinality(10)
      .|.expandAll("(n)-->(m)").withEffectiveCardinality(10)
      .|.allNodeScan("m").withEffectiveCardinality(50)
      .allNodeScan("n").withEffectiveCardinality(5)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Trail and apply WorkReduction to LHS if there is a Limit") {
    val lhsCardinality = 15
    val expandCardinality = 2
    val upperBound = 5
    val limitCount = 16
    val reductionFactor = 1 / 3.0

    // GIVEN
    val trailParameters = TrailParameters(
      min = 1,
      max = Limited(upperBound),
      start = "u",
      end = "v",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val initial = new LogicalPlanBuilder(false)
      .limit(limitCount).withCardinality(limitCount)
      .trail(trailParameters).withCardinality(limitCount / reductionFactor)
      .|.expandAll("(n)-[r]->(m)").withCardinality(expandCardinality)
      .|.argument("n").withCardinality(1)
      .allNodeScan("u").withCardinality(lhsCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    val expectedLHS = lhsCardinality * reductionFactor
    val expectedRHS = (0 until upperBound).map(Math.pow(expandCardinality, _)).sum * lhsCardinality * reductionFactor
    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(limitCount).withEffectiveCardinality(limitCount)
      .trail(trailParameters).withEffectiveCardinality(limitCount)
      .|.expandAll("(n)-[r]->(m)").withEffectiveCardinality(expandCardinality * expectedRHS)
      .|.argument("n").withEffectiveCardinality(expectedRHS)
      .allNodeScan("u").withEffectiveCardinality(expectedLHS)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Trail and apply WorkReduction to RHS if there is a Limit") {
    val lhsCardinality = 1
    val expandCardinality = 10
    val upperBound = 5
    val reductionFactor = 1 / 3.0

    // GIVEN
    val trailParameters = TrailParameters(
      min = 1,
      max = Limited(upperBound),
      start = "u",
      end = "v",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val trailCardinality = (0 to upperBound).map(Math.pow(expandCardinality, _)).sum * lhsCardinality
    val limitCount = (trailCardinality * reductionFactor).toInt
    // We need to have an integer number for limit
    assert(limitCount == trailCardinality * reductionFactor)

    val initial = new LogicalPlanBuilder(false)
      .limit(limitCount).withCardinality(limitCount)
      .trail(trailParameters).withCardinality(trailCardinality)
      .|.expandAll("(n)-[r]->(m)").withCardinality(expandCardinality)
      .|.argument("n").withCardinality(1)
      .allNodeScan("u").withCardinality(lhsCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    val expectedRHS =
      (0 until upperBound).map(Math.pow(expandCardinality * reductionFactor, _)).sum * lhsCardinality * reductionFactor
    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(limitCount).withEffectiveCardinality(limitCount)
      .trail(trailParameters).withEffectiveCardinality(trailCardinality * reductionFactor)
      .|.expandAll("(n)-[r]->(m)").withEffectiveCardinality(expandCardinality * expectedRHS)
      .|.argument("n").withEffectiveCardinality(expectedRHS)
      .allNodeScan("u").withEffectiveCardinality(lhsCardinality)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Trail and apply WorkReduction to both LHS and RHS if there is a Limit") {
    val lhsCardinality = 2
    val expandCardinality = 10
    val upperBound = 5

    val reductionFactor = 1 / 3.0
    //  Cannot apply whole reduction, since we only have 2 rows.
    val lhsReductionFactor = 1.0 / lhsCardinality
    // Remaining reduction factor, so that lhsReductionFactor * rhsReductionFactor = reductionFactor
    val rhsReductionFactor = lhsCardinality * reductionFactor

    // GIVEN
    val trailParameters = TrailParameters(
      min = 1,
      max = Limited(upperBound),
      start = "u",
      end = "v",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val trailCardinality = (0 to upperBound).map(Math.pow(expandCardinality, _)).sum * lhsCardinality
    val limitCount = (trailCardinality * reductionFactor).toInt
    // We need to have an integer number for limit
    assert(limitCount == trailCardinality * reductionFactor)

    val initial = new LogicalPlanBuilder(false)
      .limit(limitCount).withCardinality(limitCount)
      .trail(trailParameters).withCardinality(trailCardinality)
      .|.expandAll("(n)-[r]->(m)").withCardinality(expandCardinality)
      .|.argument("n").withCardinality(1)
      .allNodeScan("u").withCardinality(lhsCardinality)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    val expectedLHS = lhsCardinality * lhsReductionFactor
    val expectedRHS = (0 until upperBound).map(
      Math.pow(expandCardinality * rhsReductionFactor, _)
    ).sum * expectedLHS * rhsReductionFactor
    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(limitCount).withEffectiveCardinality(limitCount)
      .trail(trailParameters).withEffectiveCardinality(limitCount)
      .|.expandAll("(n)-[r]->(m)").withEffectiveCardinality(expandCardinality * expectedRHS)
      .|.argument("n").withEffectiveCardinality(expectedRHS)
      .allNodeScan("u").withEffectiveCardinality(1)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of Apply with Argument plan and apply WorkReduction if there is a Limit") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(100).withCardinality(100)
      .apply().withCardinality(200)
      .|.expandAll("(n)-->(m)").withCardinality(20)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(100).withEffectiveCardinality(100)
      .apply().withEffectiveCardinality(100)
      .|.expandAll("(n)-->(m)").withEffectiveCardinality(100)
      .|.argument("n").withEffectiveCardinality(5)
      .allNodeScan("n").withEffectiveCardinality(5)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should apply WorkReduction only to LHS of AssertSameNode") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(100).withCardinality(100)
      .assertSameNode("n").withCardinality(200)
      .|.allNodeScan("n").withCardinality(200)
      .allNodeScan("n").withCardinality(200)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(100).withEffectiveCardinality(100)
      .assertSameNode("n").withEffectiveCardinality(100)
      .|.allNodeScan("n").withEffectiveCardinality(200)
      .allNodeScan("n").withEffectiveCardinality(100)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should multiply RHS cardinality of ForeachApply and apply WorkReduction if there is a Limit") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(100).withCardinality(100)
      .foreachApply("n", "[1,2,3]").withCardinality(200)
      .|.expand("(n)-->(m)").withCardinality(
        10
      ) // This plan is not realistic, but necessary to differentiate the code from other ApplyPlans
      .|.setProperty("n", "prop", "5").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(200)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(100).withEffectiveCardinality(100)
      .foreachApply("n", "[1,2,3]").withEffectiveCardinality(100)
      .|.expand("(n)-->(m)").withEffectiveCardinality(1000)
      .|.setProperty("n", "prop", "5").withEffectiveCardinality(100)
      .|.argument("n").withEffectiveCardinality(100)
      .allNodeScan("n").withEffectiveCardinality(100)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should apply WorkReduction first only to RHS of Union") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(17).withCardinality(17)
      .union().withCardinality(30)
      .|.nodeByLabelScan("n", "M").withCardinality(20)
      .nodeByLabelScan("n", "N").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(17).withEffectiveCardinality(17)
      .union().withEffectiveCardinality(17)
      .|.nodeByLabelScan("n", "M").withEffectiveCardinality(7)
      .nodeByLabelScan("n", "N").withEffectiveCardinality(10)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should apply WorkReduction then also to LHS of Union") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(7).withCardinality(7)
      .union().withCardinality(30)
      .|.nodeByLabelScan("n", "M").withCardinality(20)
      .nodeByLabelScan("n", "N").withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(7).withEffectiveCardinality(7)
      .union().withEffectiveCardinality(7)
      .|.nodeByLabelScan("n", "M").withEffectiveCardinality(0)
      .nodeByLabelScan("n", "N").withEffectiveCardinality(7)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  test("Should apply WorkReduction equally to LHS and RHS of OrderedUnion") {
    // GIVEN
    val initial = new LogicalPlanBuilder(false)
      .limit(15).withCardinality(15)
      .orderedUnion("N ASC").withCardinality(30)
      .|.nodeByLabelScan("n", "M", IndexOrderAscending).withCardinality(20)
      .nodeByLabelScan("n", "N", IndexOrderAscending).withCardinality(10)

    // WHEN
    val (plan, cardinalities) = rewrite(initial, Volcano)

    // THEN
    val expected = new LogicalPlanBuilder(false)
      .limit(15).withEffectiveCardinality(15)
      .orderedUnion("N ASC").withEffectiveCardinality(15)
      .|.nodeByLabelScan("n", "M", IndexOrderAscending).withEffectiveCardinality(10)
      .nodeByLabelScan("n", "N", IndexOrderAscending).withEffectiveCardinality(5)

    val expectedPlan = expected.build()
    val expectedCards = expected.effectiveCardinalities

    (plan, cardinalities).should(haveSamePlanAndEffectiveCardinalitiesAs((expectedPlan, expectedCards)))
  }

  private def rewrite(
    pb: LogicalPlanBuilder,
    executionModel: ExecutionModel = ExecutionModel.default
  ): (LogicalPlan, EffectiveCardinalities) = {
    val plan = pb.build().endoRewrite(recordEffectiveOutputCardinality(
      executionModel,
      pb.cardinalities,
      pb.effectiveCardinalities,
      pb.providedOrders
    ))
    (plan, pb.effectiveCardinalities)
  }
}
