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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.EagerAnalyzer.unnestEager
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class unnestEagerTest extends CypherFunSuite with LogicalPlanningAttributesTestSupport with LogicalPlanningTestSupport {

  private val po_n: ProvidedOrder = ProvidedOrder.asc(varFor("n"))

  test("should unnest create from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.create(createNode("n")).withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .create(createNode("n")).withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest delete expression from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.deleteExpression("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .deleteExpression("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest delete node from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.deleteNode("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .deleteNode("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest delete path from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.deletePath("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .deletePath("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest delete relationship from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.deleteRelationship("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .deleteRelationship("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest detach delete expression from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.detachDeleteExpression("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .detachDeleteExpression("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest detach delete node from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.detachDeleteNode("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .detachDeleteNode("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest detach delete path from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.detachDeletePath("n").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .detachDeletePath("n").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set node property from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setNodeProperty("n", "prop", "5").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setNodeProperty("n", "prop", "5").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set rel property from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setRelationshipProperty("n", "prop", "5").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setRelationshipProperty("n", "prop", "5").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set generic property from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setProperty("n", "prop", "5").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setProperty("n", "prop", "5").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set node property from map from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setNodePropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setNodePropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set relationship property from map from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setRelationshipPropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setRelationshipPropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set generic property from map from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.setPropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .setPropertiesFromMap("n", "prop", removeOtherProps = false).withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest set labels from rhs of apply") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val set = SetLabels(rhs, "a", Seq.empty)
    val input = Apply(lhs, set)

    rewrite(input) should equal(SetLabels(Apply(lhs, rhs), "a", Seq.empty))
  }

  test("should unnest remove labels from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.removeLabels("n", "N").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20)
        .removeLabels("n", "N").withCardinality(20)
        .apply().withCardinality(20)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  test("should unnest Eager from rhs of apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
      .apply().withCardinality(20).withProvidedOrder(po_n)
      .|.eager().withCardinality(1)
      .|.argument("m").withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
        .eager().withCardinality(20).withProvidedOrder(po_n)
        .apply().withCardinality(20).withProvidedOrder(po_n)
        .|.argument("m").withCardinality(1)
        .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      )
  }

  implicit private class AssertableInputBuilder(inputBuilder: LogicalPlanBuilder) {
    def shouldRewriteToPlanWithAttributes(expectedBuilder: LogicalPlanBuilder): Assertion = {
      val resultPlan = rewrite(inputBuilder.build(), inputBuilder.cardinalities, inputBuilder.providedOrders,inputBuilder.idGen)
      (resultPlan, inputBuilder.cardinalities) should haveSameCardinalitiesAs((expectedBuilder.build(), expectedBuilder.cardinalities))
      (resultPlan, inputBuilder.providedOrders) should haveSameProvidedOrdersAs((expectedBuilder.build(), expectedBuilder.providedOrders))
    }
  }

  private def rewrite(p: LogicalPlan, cardinalities: Cardinalities, providedOrders: ProvidedOrders, idGen: IdGen): LogicalPlan = {
    val unnest = unnestEager(
      new StubSolveds,
      cardinalities,
      providedOrders,
      Attributes(idGen)
    )
    p.endoRewrite(unnest)
  }

  private def stubCardinalities(): StubCardinalities = new StubCardinalities {
    override def defaultValue: Cardinality = Cardinality.SINGLE
  }

  private def rewrite(p: LogicalPlan): LogicalPlan = rewrite(p, stubCardinalities(), new StubProvidedOrders, idGen)
}
