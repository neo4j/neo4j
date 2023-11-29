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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy.ComparingProvidedAttributesOnly
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.NonEmptyProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.exceptions.TestFailedException

class LogicalPlanningAttributesTestSupportTest
    extends CypherFunSuite
    with LogicalPlanningTestSupport2
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanningAttributesTestSupport {

  val config: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .build()

  val query: String =
    """MATCH (a:A)
      |WHERE a.prop > 42
      |RETURN a
      |ORDER BY a.prop
      |LIMIT 2""".stripMargin

  val planState: LogicalPlanState =
    config.planState(query)

  val providedOrder: NonEmptyProvidedOrder =
    ProvidedOrder.asc(prop("a", "prop"), Map(v"a" -> varFor("a"))).fromLeft

  val planAndEffectiveCardinalities: (LogicalPlan, PlanningAttributes.EffectiveCardinalities) =
    (planState.logicalPlan, planState.planningAttributes.effectiveCardinalities)

  val planAndProvidedOrders: (LogicalPlan, PlanningAttributes.ProvidedOrders) =
    (planState.logicalPlan, planState.planningAttributes.providedOrders)

  test("should accept correct cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withCardinality(2)
        .top(2, "`a.prop` ASC").withCardinality(2)
        .projection("cacheN[a.prop] AS `a.prop`").withCardinality(15)
        .filter("cacheNFromStore[a.prop] > 42").withCardinality(15)
        .nodeByLabelScan("a", "A", IndexOrderNone).withCardinality(50)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test("should reject incorrect cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withCardinality(2)
        .top(2, "`a.prop` ASC").withCardinality(2)
        .projection("cacheN[a.prop] AS `a.prop`").withCardinality(1000)
        .filter("cacheNFromStore[a.prop] > 42").withCardinality(15)
        .nodeByLabelScan("a", "A", IndexOrderNone).withCardinality(50)

    assertThrows[TestFailedException](planState should haveSamePlanAndCardinalitiesAsBuilder(expected))
  }

  test("should reject incorrect default cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withCardinality(2)
        .top(2, "`a.prop` ASC").withCardinality(2)
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter("cacheNFromStore[a.prop] > 42").withCardinality(15)
        .nodeByLabelScan("a", "A", IndexOrderNone)

    assertThrows[TestFailedException](planState should haveSamePlanAndCardinalitiesAsBuilder(expected))
  }

  test("should accept correct cardinalities where provided") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withCardinality(2)
        .top(2, "`a.prop` ASC")
        .projection("cacheN[a.prop] AS `a.prop`").withCardinality(15)
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withCardinality(50)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expected, ComparingProvidedAttributesOnly)
  }

  test("should reject incorrect provided cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withCardinality(2)
        .top(2, "`a.prop` ASC")
        .projection("cacheN[a.prop] AS `a.prop`").withCardinality(1000)
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withCardinality(50)

    assertThrows[TestFailedException] {
      planState should haveSamePlanAndCardinalitiesAsBuilder(expected, ComparingProvidedAttributesOnly)
    }
  }

  test("should accept correct effective cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withEffectiveCardinality(2)
        .top(2, "`a.prop` ASC").withEffectiveCardinality(2)
        .projection("cacheN[a.prop] AS `a.prop`").withEffectiveCardinality(15)
        .filter("cacheNFromStore[a.prop] > 42").withEffectiveCardinality(15)
        .nodeByLabelScan("a", "A", IndexOrderNone).withEffectiveCardinality(50)

    planAndEffectiveCardinalities should haveSamePlanAndEffectiveCardinalitiesAs((
      expected.build(),
      expected.effectiveCardinalities
    ))
  }

  test("should reject incorrect effective cardinalities – more like defective cardinalities") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withEffectiveCardinality(2)
        .top(2, "`a.prop` ASC")
        .projection("cacheN[a.prop] AS `a.prop`").withEffectiveCardinality(1000)
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withEffectiveCardinality(50)

    assertThrows[TestFailedException] {
      planAndEffectiveCardinalities should haveSamePlanAndEffectiveCardinalitiesAs(
        (expected.build(), expected.effectiveCardinalities),
        ComparingProvidedAttributesOnly
      )
    }
  }

  test("should accept correct provided orders") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withProvidedOrder(providedOrder)
        .top(2, "`a.prop` ASC").withProvidedOrder(providedOrder)
        .projection("cacheN[a.prop] AS `a.prop`").withProvidedOrder(NoProvidedOrder)
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withProvidedOrder(NoProvidedOrder)

    planAndProvidedOrders should haveSamePlanAndProvidedOrdersAs((expected.build(), expected.providedOrders))
  }

  test("should reject incorrect provided orders") {
    val expected =
      config
        .planBuilder()
        .produceResults("a").withProvidedOrder(providedOrder)
        .top(2, "`a.prop` ASC").withProvidedOrder(providedOrder)
        .projection("cacheN[a.prop] AS `a.prop`").withProvidedOrder(NoProvidedOrder)
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withProvidedOrder(ProvidedOrder.desc(varFor("nonsense")).fromRight)

    assertThrows[TestFailedException] {
      planAndProvidedOrders should haveSamePlanAndProvidedOrdersAs((expected.build(), expected.providedOrders))
    }
  }

  test("should accept correct provided orders where provided (hah)") {
    val expected =
      config
        .planBuilder()
        .produceResults("a")
        .top(2, "`a.prop` ASC").withProvidedOrder(providedOrder)
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withProvidedOrder(NoProvidedOrder)

    planAndProvidedOrders should haveSamePlanAndProvidedOrdersAs(
      (expected.build(), expected.providedOrders),
      ComparingProvidedAttributesOnly
    )
  }

  test("should reject incorrect provided orders where provided") {
    val expected =
      config
        .planBuilder()
        .produceResults("a")
        .top(2, "`a.prop` ASC").withProvidedOrder(ProvidedOrder.desc(varFor("nonsense")).fromRight)
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter("cacheNFromStore[a.prop] > 42")
        .nodeByLabelScan("a", "A", IndexOrderNone).withProvidedOrder(NoProvidedOrder)

    assertThrows[TestFailedException] {
      planAndProvidedOrders should haveSamePlanAndProvidedOrdersAs(
        (expected.build(), expected.providedOrders),
        ComparingProvidedAttributesOnly
      )
    }
  }
}
