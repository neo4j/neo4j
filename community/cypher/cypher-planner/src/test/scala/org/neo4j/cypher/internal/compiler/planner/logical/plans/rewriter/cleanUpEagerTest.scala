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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class cleanUpEagerTest extends CypherFunSuite
    with LogicalPlanningAttributesTestSupport
    with LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {
  val DEFAULT_BUFFER_SIZE_4MB: Int = 4 * 1024 * 1024

  test("should concatenate two eagers after eachother") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(1)
      .eager().withCardinality(1)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(1)
      .argument().withCardinality(1)
  }

  test("should not move eager below unwind") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .unwind("[1,2,3] AS i").withCardinality(1)
      .eager().withCardinality(1)
      .argument().withCardinality(1)

    inputBuilder.shouldNotRewritePlan
  }

  test("should move eager on top of unwind to below it") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(3)
      .unwind("[1,2,3] AS i").withCardinality(3)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .unwind("[1,2,3] AS i").withCardinality(3)
      .eager().withCardinality(1)
      .argument().withCardinality(1)
  }

  test("should move eager on top of unwind to below it repeatedly") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(27)
      .unwind("[1,2,3] AS k").withCardinality(27)
      .eager().withCardinality(9)
      .unwind("[1,2,3] AS j").withCardinality(9)
      .eager().withCardinality(3)
      .unwind("[1,2,3] AS i").withCardinality(3)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .unwind("[1,2,3] AS k").withCardinality(27)
      .unwind("[1,2,3] AS j").withCardinality(9)
      .unwind("[1,2,3] AS i").withCardinality(3)
      .eager().withCardinality(1)
      .argument().withCardinality(1)
  }

  test("should move eager on top of load csv to below it") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(3)
      .loadCSV("$url", "a", NoHeaders).withCardinality(3)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .loadCSV("$url", "a", NoHeaders).withCardinality(3)
      .eager().withCardinality(1)
      .argument().withCardinality(1)
  }

  test("should move limit on top of eager to below it") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .limit(10).withCardinality(10)
      .eager().withCardinality(100)
      .argument().withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(10)
      .limit(10).withCardinality(10)
      .argument().withCardinality(100)
  }

  test("should not rewrite plan with eager below load csv") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .loadCSV("$url", "a", NoHeaders).withCardinality(3)
      .eager().withCardinality(1)
      .argument().withCardinality(1)

    inputBuilder.shouldNotRewritePlan
  }

  test("should use exhaustive limit when moving eager on top of limit when there are updates") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .limit(10).withCardinality(10)
      .eager().withCardinality(100)
      .create(createNode("n")).withCardinality(100)
      .argument().withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(10)
      .exhaustiveLimit(10).withCardinality(10)
      .create(createNode("n")).withCardinality(100)
      .argument().withCardinality(100)
  }

  test("should remove eager on top of eager aggregation") {
    val inputBuilder = new LogicalPlanBuilder(wholePlan = false)
      .eager().withCardinality(1)
      .aggregation(Seq.empty, Seq.empty).withCardinality(1)
      .argument().withCardinality(10)

    inputBuilder shouldRewriteToPlanWithAttributes new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq.empty).withCardinality(1)
      .argument().withCardinality(10)
  }

  implicit private class AssertableInputBuilder(inputBuilder: LogicalPlanBuilder) {

    def shouldRewriteToPlanWithAttributes(expectedBuilder: LogicalPlanBuilder): Assertion = {
      val resultPlan =
        rewrite(inputBuilder.build(), inputBuilder.cardinalities, inputBuilder.idGen)
      (resultPlan, inputBuilder.cardinalities) should haveSamePlanAndCardinalitiesAs((
        expectedBuilder.build(),
        expectedBuilder.cardinalities
      ))
      (resultPlan, inputBuilder.providedOrders) should haveSamePlanAndProvidedOrdersAs((
        expectedBuilder.build(),
        expectedBuilder.providedOrders
      ))
    }

    def shouldNotRewritePlan: Assertion = {
      val inputPlan = inputBuilder.build()
      val resultPlan = rewrite(inputPlan, inputBuilder.cardinalities, inputBuilder.idGen)
      resultPlan shouldEqual inputPlan
    }
  }

  private def rewrite(p: LogicalPlan, cardinalities: Cardinalities, idGen: IdGen): LogicalPlan =
    fixedPoint(CancellationChecker.neverCancelled())((p: LogicalPlan) =>
      p.endoRewrite(cleanUpEager(cardinalities, Attributes(idGen)))
    )(p)
}
