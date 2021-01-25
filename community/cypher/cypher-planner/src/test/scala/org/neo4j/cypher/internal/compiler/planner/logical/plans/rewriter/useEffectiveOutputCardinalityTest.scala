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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class useEffectiveOutputCardinalityTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  val leafCardinality = 10

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
    plan shouldEqual initialPlan.build() // assert that shape is the same

    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("n"), cardinality = limit, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("m"), cardinality = limit, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[CartesianProduct](args = Seq(), cardinality = limit, plan, cardinalities)
    shouldIncludePlanWithCardinality[Limit](args = Seq(), cardinality = limit, plan, cardinalities)
    shouldIncludePlanWithCardinality[ProduceResult](args = Seq(), cardinality = limit, plan, cardinalities)
  }

  test("Should not update past earlier limit") {
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
    plan shouldEqual initialPlan.build() // assert that shape is the same

    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("n"), cardinality = leafCardinality, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("m"), cardinality = leafCardinality, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[CartesianProduct](args = Seq(), cardinality = lowLimit, plan, cardinalities)
    shouldIncludePlanWithCardinality[Limit](args = Seq(), cardinality = lowLimit, plan, cardinalities)
    shouldIncludePlanWithCardinality[ProduceResult](args = Seq(), cardinality = lowLimit, plan, cardinalities)
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
    plan shouldEqual initialPlan.build() // assert that shape is the same

    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("m"), cardinality = leafCardinality, plan, cardinalities)
    shouldIncludePlanWithCardinality[AllNodesScan](args = Seq("n"), cardinality = leafCardinality, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[NodeHashJoin](args = Seq(), cardinality = leafCardinality, plan, cardinalities, lessThan = true)
    shouldIncludePlanWithCardinality[Limit](args = Seq(), cardinality = limit, plan, cardinalities)
    shouldIncludePlanWithCardinality[ProduceResult](args = Seq(), cardinality = limit, plan, cardinalities)
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
    plan shouldEqual initialPlan.build() // assert that shape is the same

    shouldIncludePlanWithCardinality[Create](args = Seq(), cardinality = 100, plan, cardinalities)
    shouldIncludePlanWithCardinality[ExhaustiveLimit](args = Seq(), cardinality = 10, plan, cardinalities)
    shouldIncludePlanWithCardinality[ProduceResult](args = Seq(), cardinality = 10, plan, cardinalities)
  }

  private def shouldIncludePlanWithCardinality[T <: LogicalPlan : Manifest](args: Seq[Any],
                                                                            cardinality: Double,
                                                                            plan: LogicalPlan,
                                                                            cardinalities: Cardinalities,
                                                                            lessThan: Boolean = false) = {
    val maybePlan = plan.treeFind[LogicalPlan] { case p: T if args.forall(a => p.productIterator.contains(a)) => true }

    maybePlan should not be None

    if (lessThan) {
      cardinalities(maybePlan.get.id) should be < Cardinality(cardinality)
    } else {
      cardinalities(maybePlan.get.id) shouldEqual Cardinality(cardinality)
    }
  }

  private def rewrite(pb: LogicalPlanBuilder): (LogicalPlan, Cardinalities) = {
    val plan = pb.build().endoRewrite(useEffectiveOutputCardinality(pb.cardinalities, noAttributes(pb.idGen)))
    (plan, pb.cardinalities)
  }
}
