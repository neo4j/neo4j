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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.PropertyAndPredicateType
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.extractVariableForValue
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.providedOrderForLabelScan
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.providedOrderForRelationshipTypeScan
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.indexOrder
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.indexPropertyXFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.indexPropertyXFooExact
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingAscX
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingAscXFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingAscY
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingDescX
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingDescXFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.interestingDescY
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.requiredAscX
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.requiredAscXFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.requiredDescX
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.requiredDescXFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.wFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.x
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.xFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.y
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.yFoo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrderingTest.zFoo
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.OrderCandidate
import org.neo4j.cypher.internal.ir.ordering.OrderCandidateFactory
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.Matcher

abstract class ResultOrderingTest[OC <: OrderCandidate[OC]](
  ascXFoo: InterestingOrder,
  descXFoo: InterestingOrder,
  ascX: InterestingOrder,
  descX: InterestingOrder,
  orderCandidateFactory: OrderCandidateFactory[OC],
  toInterestingOrder: OC => InterestingOrder
) extends CypherFunSuite with LogicalPlanningTestSupport2 {

  // Index operator

  protected val beEmpty: Matcher[Any] = be((ProvidedOrder.empty, IndexOrderNone))

  test("IndexOperator: No interesting order results in no order") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, BOTH) should beEmpty
    indexOrder(InterestingOrder.empty, indexPropertyXFooExact, BOTH) should beEmpty
  }

  test("IndexOperator: Single property no capability results in empty provided order") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, NONE) should beEmpty
    indexOrder(descXFoo, indexPropertyXFoo, NONE) should beEmpty
    indexOrder(ascXFoo, indexPropertyXFooExact, NONE) should beEmpty
  }

  test("IndexOperator: Single property order results in matching provided order for compatible index capability") {
    indexOrder(ascXFoo, indexPropertyXFoo, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))
    indexOrder(ascXFoo, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))
    indexOrder(descXFoo, indexPropertyXFoo, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderDescending))
    indexOrder(descXFoo, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderAscending))
  }

  test(
    "IndexOperator: Single property order with projected property results in matching provided order for compatible index capability"
  ) {
    val interestingAsc = toInterestingOrder(orderCandidateFactory.asc(v"xfoo", Map(v"xfoo" -> xFoo)))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))
    indexOrder(interestingAsc, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))

    val interestingDesc = toInterestingOrder(orderCandidateFactory.desc(v"xfoo", Map(v"xfoo" -> xFoo)))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderDescending))
    // Since the property has an exact predicate the ascending and descending order is the same, so we can choose both index orders and ascending is cheaper
    indexOrder(interestingDesc, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderAscending))
  }

  test(
    "IndexOperator: Single property order with projected node results in matching provided order for compatible index capability"
  ) {
    val interestingAsc = toInterestingOrder(orderCandidateFactory.asc(yFoo, Map(y -> x)))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))
    indexOrder(interestingAsc, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.asc(xFoo), IndexOrderAscending))

    val interestingDesc = toInterestingOrder(orderCandidateFactory.desc(yFoo, Map(y -> x)))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderDescending))
    // Since the property has an exact predicate the ascending and descending order is the same, so we can choose both index orders and ascending is cheaper
    indexOrder(interestingDesc, indexPropertyXFooExact, BOTH) should be((ProvidedOrder.desc(xFoo), IndexOrderAscending))
  }

  test(
    "IndexOperator: Multi property order results in matching provided order for compatible index capability (no exact predicates)"
  ) {
    val properties = Seq("x", "y", "z").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo),
      IndexOrderAscending
    ))
  }

  test(
    "IndexOperator: Multi property order results in matching provided order for compatible index capability (all exact predicates)"
  ) {
    val properties = Seq("x", "y", "z").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = true)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo),
      IndexOrderAscending
    ))
  }

  test(
    "IndexOperator: Multi property order results in provided order if property order does not match (no exact predicates)"
  ) {
    val properties = Seq("y", "x", "z").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should beEmpty
  }

  test(
    "IndexOperator: Multi property order results in provided order if property order does not match (all exact predicates)"
  ) {
    val properties = Seq("y", "x", "z").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = true)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should beEmpty
  }

  test(
    "IndexOperator: Multi property order results in provided order if property order partially matches (no exact predicates)"
  ) {
    val properties = Seq("x", "z", "y").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(zFoo).asc(yFoo),
      IndexOrderAscending
    ))
  }

  test(
    "IndexOperator: Multi property order results in provided order if property order partially matches (all exact predicates)"
  ) {
    val properties = Seq("x", "z", "y").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = true)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(zFoo).asc(yFoo),
      IndexOrderAscending
    ))
  }

  test("IndexOperator: Multi property order results in provided order if mixed sort direction (no exact predicates)") {
    val properties = Seq("x", "y", "z", "w").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).desc(zFoo).asc(wFoo))

    // Index can only give full ascending or descending, not a mixture. Therefore we follow the first required order
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
  }

  test(
    "IndexOperator: Multi property order results in provided order if mixed sort direction (some exact predicates)"
  ) {
    def getPropAndType(isExactPredicates: Seq[Boolean]): Seq[PropertyAndPredicateType] = {
      val properties = Seq(xFoo, yFoo, zFoo, wFoo)
      isExactPredicates.zip(properties).map { case (isExactPredicate, prop) =>
        PropertyAndPredicateType(prop, isExactPredicate)
      }
    }
    val properties1 = getPropAndType(Seq(true, false, false, false))
    val properties2 = getPropAndType(Seq(true, true, false, false))
    val properties3 = getPropAndType(Seq(true, true, true, false))
    val properties4 = getPropAndType(Seq(true, false, true, false))
    val properties5 = getPropAndType(Seq(false, true, false, false))
    val properties6 = getPropAndType(Seq(true, true, false, true))

    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).desc(zFoo).asc(wFoo))

    // Index can only give full ascending or descending, not a mixture. Therefore we follow the first required order
    indexOrder(interestingOrder, properties1, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
    indexOrder(interestingOrder, properties2, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).desc(zFoo).desc(wFoo),
      IndexOrderDescending
    ))
    indexOrder(interestingOrder, properties3, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).desc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
    indexOrder(interestingOrder, properties4, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).desc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
    indexOrder(interestingOrder, properties5, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
    indexOrder(interestingOrder, properties6, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).desc(zFoo).asc(wFoo),
      IndexOrderDescending
    ))
  }

  test("IndexOperator: Shorter multi property order results in provided order (no exact predicates)") {
    val properties = Seq("x", "y", "z", "w").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
  }

  test("IndexOperator: Shorter multi property order results in provided order (all exact predicates)") {
    val properties = Seq("x", "y", "z", "w").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = true)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((
      ProvidedOrder.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo),
      IndexOrderAscending
    ))
  }

  test("IndexOperator: Longer multi property order results in partial matching provided order (no exact predicates)") {
    val properties = Seq("x", "y").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((ProvidedOrder.asc(xFoo).asc(yFoo), IndexOrderAscending))
  }

  test("IndexOperator: Longer multi property order results in partial matching provided order (all exact predicates)") {
    val properties = Seq("x", "y").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = true)
    }
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(xFoo).asc(yFoo).asc(zFoo).asc(wFoo))
    indexOrder(interestingOrder, properties, BOTH) should be((ProvidedOrder.asc(xFoo).asc(yFoo), IndexOrderAscending))
  }

  // Label scan

  test("Label scan: Empty order results in empty provided order") {
    providedOrderForLabelScan(InterestingOrder.empty, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.empty)
  }

  test("Label scan: No order capability results in empty provided order") {
    providedOrderForLabelScan(ascX, x, IndexOrderCapability.NONE) should be(ProvidedOrder.empty)
  }

  test("Label scan: Simple order results in matching provided order") {
    providedOrderForLabelScan(ascX, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.asc(x))
    providedOrderForLabelScan(descX, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.desc(x))
  }

  test("Label scan: Simple order with projected variable results in matching provided order") {
    val interestingAsc = toInterestingOrder(orderCandidateFactory.asc(v"blob", Map(v"blob" -> x)))
    providedOrderForLabelScan(interestingAsc, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.asc(x))

    val interestingDesc = toInterestingOrder(orderCandidateFactory.desc(v"blob", Map(v"blob" -> x)))
    providedOrderForLabelScan(interestingDesc, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.desc(x))
  }

  test("Label scan: Multi variable order results in matching provided order") {
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(x).asc(y))
    providedOrderForLabelScan(interestingOrder, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.asc(x))
  }

  test("Label scan: Multi variable order results in empty provided order if variable order does not match") {
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(y).asc(x))
    providedOrderForLabelScan(interestingOrder, x, IndexOrderCapability.BOTH) should be(ProvidedOrder.empty)
  }

  test("Label scan: results in empty provided order when it can't be fulfilled") {
    providedOrderForLabelScan(
      toInterestingOrder(orderCandidateFactory.asc(xFoo)),
      x,
      IndexOrderCapability.BOTH
    ) should be(ProvidedOrder.empty)
    providedOrderForLabelScan(
      toInterestingOrder(orderCandidateFactory.desc(xFoo)),
      x,
      IndexOrderCapability.BOTH
    ) should be(ProvidedOrder.empty)
  }

  // RelType scan

  test("RelType scan: Empty order results in empty provided order") {
    providedOrderForRelationshipTypeScan(InterestingOrder.empty, v"x", IndexOrderCapability.BOTH) should be(
      ProvidedOrder.empty
    )
  }

  test("RelType scan: No order capability results in empty provided order") {
    providedOrderForRelationshipTypeScan(ascX, v"x", IndexOrderCapability.NONE) should be(ProvidedOrder.empty)
  }

  test("RelType scan: Simple order results in matching provided order") {
    providedOrderForRelationshipTypeScan(ascX, v"x", IndexOrderCapability.BOTH) should be(ProvidedOrder.asc(x))
    providedOrderForRelationshipTypeScan(descX, v"x", IndexOrderCapability.BOTH) should be(ProvidedOrder.desc(x))
  }

  test("RelType scan: Simple order with projected variable results in matching provided order") {
    val interestingAsc = toInterestingOrder(orderCandidateFactory.asc(v"blob", Map(v"blob" -> x)))
    providedOrderForRelationshipTypeScan(interestingAsc, v"x", IndexOrderCapability.BOTH) should be(
      ProvidedOrder.asc(x)
    )

    val interestingDesc = toInterestingOrder(orderCandidateFactory.desc(v"blob", Map(v"blob" -> x)))
    providedOrderForRelationshipTypeScan(interestingDesc, v"x", IndexOrderCapability.BOTH) should be(
      ProvidedOrder.desc(x)
    )
  }

  test("RelType scan: Multi variable order results in matching provided order") {
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(x).asc(y))
    providedOrderForRelationshipTypeScan(interestingOrder, v"x", IndexOrderCapability.BOTH) should be(
      ProvidedOrder.asc(x)
    )
  }

  test("RelType scan: Multi variable order results in empty provided order if variable order does not match") {
    val interestingOrder = toInterestingOrder(orderCandidateFactory.asc(y).asc(x))
    providedOrderForRelationshipTypeScan(interestingOrder, v"x", IndexOrderCapability.BOTH) should be(
      ProvidedOrder.empty
    )
  }

  test("RelType scan: results in empty provided order when it can't be fulfilled") {
    providedOrderForRelationshipTypeScan(
      toInterestingOrder(orderCandidateFactory.asc(xFoo)),
      v"x",
      IndexOrderCapability.BOTH
    ) should be(
      ProvidedOrder.empty
    )
    providedOrderForRelationshipTypeScan(
      toInterestingOrder(orderCandidateFactory.desc(xFoo)),
      v"x",
      IndexOrderCapability.BOTH
    ) should be(
      ProvidedOrder.empty
    )
  }

  // Extract Variable for Value

  test("extractVariableForValue") {
    extractVariableForValue(x, Map.empty) should be(Some(x))
    extractVariableForValue(prop("x", "prop"), Map.empty) should be(None)
    extractVariableForValue(prop("x", "prop"), Map(v"x" -> y)) should be(None)
    extractVariableForValue(x, Map(v"x" -> prop("y", "prop"))) should be(None)
    extractVariableForValue(x, Map(v"x" -> v"z", v"z" -> prop("y", "prop"))) should be(None)
    extractVariableForValue(x, Map(v"x" -> v"z")) should be(Some(v"z"))
  }
}

object ResultOrderingTest extends AstConstructionTestSupport {
  val xFoo: Property = prop("x", "foo")
  val yFoo: Property = prop("y", "foo")
  val zFoo: Property = prop("z", "foo")
  val wFoo: Property = prop("w", "foo")

  val indexPropertyXFoo: Seq[PropertyAndPredicateType] =
    Seq(PropertyAndPredicateType(xFoo, isSingleExactPredicate = false))

  val indexPropertyXFooExact: Seq[PropertyAndPredicateType] =
    Seq(PropertyAndPredicateType(xFoo, isSingleExactPredicate = true))
  val x: Variable = v"x"
  val y: Variable = v"y"

  val requiredAscXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo))
  val requiredDescXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(xFoo))
  val requiredAscX: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(x))
  val requiredDescX: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(x))

  val interestingAscXFoo: InterestingOrderCandidate = InterestingOrderCandidate.asc(xFoo)
  val interestingDescXFoo: InterestingOrderCandidate = InterestingOrderCandidate.desc(xFoo)
  val interestingAscX: InterestingOrderCandidate = InterestingOrderCandidate.asc(x)
  val interestingDescX: InterestingOrderCandidate = InterestingOrderCandidate.desc(x)
  val interestingAscY: InterestingOrderCandidate = InterestingOrderCandidate.asc(y)
  val interestingDescY: InterestingOrderCandidate = InterestingOrderCandidate.desc(y)

  def indexOrder(
    interestingOrder: InterestingOrder,
    indexProperties: Seq[PropertyAndPredicateType],
    orderCapability: IndexOrderCapability
  ): (ProvidedOrder, IndexOrder) =
    ResultOrdering.providedOrderForIndexOperator(
      interestingOrder,
      indexProperties,
      orderCapability
    )
}

class RequiredTestIndexOrder extends ResultOrderingTest[RequiredOrderCandidate](
      requiredAscXFoo,
      requiredDescXFoo,
      requiredAscX,
      requiredDescX,
      RequiredOrderCandidate,
      InterestingOrder.required
    )

class InterestingTestIndexOrder extends ResultOrderingTest[InterestingOrderCandidate](
      InterestingOrder.interested(interestingAscXFoo),
      InterestingOrder.interested(interestingDescXFoo),
      InterestingOrder.interested(interestingAscX),
      InterestingOrder.interested(interestingDescX),
      InterestingOrderCandidate,
      InterestingOrder.interested
    ) {
  // Index operator

  test("IndexOperator: Single property empty provided order when there is no capability") {
    indexOrder(requiredDescXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, NONE) should be((
      ProvidedOrder.empty,
      IndexOrderNone
    ))
    indexOrder(requiredAscXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, NONE) should be((
      ProvidedOrder.empty,
      IndexOrderNone
    ))
  }

  test("IndexOperator: Multi property empty provided order when there is no capability") {
    val properties = Seq("x", "y").map { node =>
      PropertyAndPredicateType(prop(node, "foo"), isSingleExactPredicate = false)
    }

    val interestingDesc =
      InterestingOrder.interested(interestingDescXFoo.desc(yFoo)).interesting(interestingAscXFoo.asc(yFoo))
    indexOrder(interestingDesc, properties, NONE) should be((ProvidedOrder.empty, IndexOrderNone))

    val interestingAsc =
      InterestingOrder.interested(interestingAscXFoo.asc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingAsc, properties, NONE) should be((ProvidedOrder.empty, IndexOrderNone))
  }

  // Label scan

  test("Label scan: Single variable interesting order results in provided order when required can't be fulfilled") {
    providedOrderForLabelScan(requiredDescX.interesting(interestingAscY), y, IndexOrderCapability.BOTH) should be(
      ProvidedOrder.asc(y)
    )
    providedOrderForLabelScan(requiredAscX.interesting(interestingDescY), y, IndexOrderCapability.BOTH) should be(
      ProvidedOrder.desc(y)
    )
  }

  test(
    "Label scan: results in empty provided order when neither required nor interesting can be fulfilled or are empty"
  ) {
    providedOrderForLabelScan(
      requiredDescXFoo.interesting(interestingDescXFoo),
      x,
      IndexOrderCapability.BOTH
    ) should be(ProvidedOrder.empty)
    providedOrderForLabelScan(requiredAscXFoo.interesting(interestingAscXFoo), x, IndexOrderCapability.BOTH) should be(
      ProvidedOrder.empty
    )
  }
}
