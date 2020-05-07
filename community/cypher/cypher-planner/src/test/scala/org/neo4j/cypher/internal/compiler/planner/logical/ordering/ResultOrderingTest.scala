/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.extractVariableForValue
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.providedOrderForLabelScan
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.ASC
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.DESC
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ResultOrderingTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val xFoo: Property = prop("x", "foo")
  private val yFoo: Property = prop("y", "foo")
  private val indexPropertyXFoo: Seq[Property] = Seq(xFoo)
  private val x = varFor("x")
  private val y = varFor("y")

  private val requiredAscXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo))
  private val requiredDescXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(xFoo))
  private val requiredAscX: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(x))
  private val requiredDescX: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(x))
  private val requiredDescYFoo = InterestingOrder.required(RequiredOrderCandidate.desc(yFoo))

  private val interestingAscXFoo: InterestingOrderCandidate = InterestingOrderCandidate.asc(xFoo)
  private val interestingDescXFoo: InterestingOrderCandidate = InterestingOrderCandidate.desc(xFoo)
  private val interestingAscX: InterestingOrderCandidate = InterestingOrderCandidate.asc(x)
  private val interestingDescX: InterestingOrderCandidate = InterestingOrderCandidate.desc(x)
  private val interestingAscY: InterestingOrderCandidate = InterestingOrderCandidate.asc(y)
  private val interestingDescY: InterestingOrderCandidate = InterestingOrderCandidate.desc(y)


  // Index operator

  test("IndexOperator: Empty required order results in provided order of index order capability ascending") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
  }

  test("IndexOperator: Single property required DESC still results in provided ASC if index is not capable of DESC") {
    indexOrder(requiredDescXFoo, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
  }

  test("IndexOperator: Single property required ASC results in provided DESC if index is not capable of ASC") {
    indexOrder(requiredAscXFoo, indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property no capability results in empty provided order") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(requiredDescYFoo, indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
  }

  test("IndexOperator: Single property required order results in matching provided order for compatible index capability") {
    indexOrder(requiredAscXFoo, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
    indexOrder(requiredDescXFoo, indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property required order with projected property results in matching provided order for compatible index capability") {
    val interestingAsc = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), Map("xfoo" -> xFoo)))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(xFoo))

    val interestingDesc = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), Map("xfoo" -> xFoo)))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property required order with projected node results in matching provided order for compatible index capability") {
    val interestingAsc = InterestingOrder.required(RequiredOrderCandidate.asc(yFoo, Map("y" -> varFor("x"))))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(xFoo))

    val interestingDesc = InterestingOrder.required(RequiredOrderCandidate.desc(yFoo, Map("y" -> varFor("x"))))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Multi property required order results in matching provided order for compatible index capability") {
    val properties = Seq("x", "y", "z").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo).asc(prop("z", "foo")))
  }

  test("IndexOperator: Multi property required order results in provided order if property order does not match") {
    val properties = Seq("y", "x", "z").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(yFoo).asc(xFoo).asc(prop("z", "foo")))
  }

  test("IndexOperator: Multi property required order results in provided order if property order partially matches") {
    val properties = Seq("x", "z", "y").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(prop("z", "foo")).asc(yFoo))
  }

  test("IndexOperator: Multi property required order results in provided order if mixed sort direction") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo).desc(prop("z", "foo")).asc(prop("w", "foo")))

    // Index can only give full ascending or descending, not a mixture. Therefore we follow the first required order
    indexOrder(interestingOrder, properties, BOTH) should be(ProvidedOrder.asc(xFoo).asc(yFoo).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("IndexOperator: Shorter multi property required order results in provided order") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("IndexOperator: Longer multi property required order results in partial matching provided order") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo).asc(prop("z", "foo")).asc(prop("w", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))
  }

  // Test the interesting part of the InterestingOrder

  test("IndexOperator: Single property interesting order results in provided order when required can't be fulfilled or is empty") {
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))

    indexOrder(requiredDescXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
    indexOrder(requiredAscXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property interesting order with projection results in matching provided order for compatible index capability") {
    val propAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), Map("xfoo" -> xFoo)))
    indexOrder(propAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(xFoo))

    val propDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo"), Map("xfoo" -> xFoo)))
    indexOrder(propDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(xFoo))


    val varAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(yFoo, Map("y" -> varFor("x"))))
    indexOrder(varAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(xFoo))

    val varDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(yFoo, Map("y" -> varFor("x"))))
    indexOrder(varDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))

    indexOrder(requiredDescXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(xFoo))
    indexOrder(requiredAscXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(xFoo))
  }

  test("IndexOperator: Single property empty provided order when there is no capability") {
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)

    indexOrder(requiredDescXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(requiredAscXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
  }

  test("IndexOperator: Multi property interesting order results in provided order when required can't be fulfilled or is empty") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    // can't fulfill first interesting so falls back on second interesting
    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(yFoo)).interesting(interestingAscXFoo.asc(yFoo))
    indexOrder(interestingDesc, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo.asc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingAsc, properties, DESC) should be(ProvidedOrder.desc(xFoo).desc(yFoo))

    // can't fulfill required so falls back on interesting
    val interestingDescRequired = InterestingOrder.required(RequiredOrderCandidate.desc(xFoo).desc(yFoo)).interesting(interestingAscXFoo.asc(yFoo))
    indexOrder(interestingDescRequired, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))

    val interestingAscRequired = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingAscRequired, properties, DESC) should be(ProvidedOrder.desc(xFoo).desc(yFoo))
  }

  test("IndexOperator: Multi property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingDesc, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo).interesting(InterestingOrderCandidate.asc(yFoo))
    indexOrder(interestingAsc, properties, DESC) should be(ProvidedOrder.desc(xFoo).desc(yFoo))


    val interestingDescRequired = InterestingOrder.required(RequiredOrderCandidate.desc(xFoo).desc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingDescRequired, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))

    val interestingAscRequired = InterestingOrder.required(RequiredOrderCandidate.asc(xFoo).asc(yFoo)).interesting(interestingAscXFoo.asc(yFoo))
    indexOrder(interestingAscRequired, properties, DESC) should be(ProvidedOrder.desc(xFoo).desc(yFoo))
  }

  test("IndexOperator: Multi property empty provided order when there is no capability") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(yFoo)).interesting(interestingAscXFoo.asc(yFoo))
    indexOrder(interestingDesc, properties, NONE) should be(ProvidedOrder.empty)

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo.asc(yFoo)).interesting(interestingDescXFoo.desc(yFoo))
    indexOrder(interestingAsc, properties, NONE) should be(ProvidedOrder.empty)
  }

  test("IndexOperator: Multi property interesting order results in provided order if mixed sort direction") {
    val properties = Seq("x", "y", "z").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingDescXFoo.asc(yFoo).desc(prop("z", "foo")))
    indexOrder(interesting, properties, BOTH) should be(ProvidedOrder.desc(xFoo).desc(yFoo).desc(prop("z", "foo")))
  }

  test("IndexOperator: Shorter multi property interesting order results in provided order") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingAscXFoo.asc(yFoo))
    indexOrder(interesting, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("IndexOperator: Longer multi property interesting order results in partial matching provided order") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingAscXFoo.asc(yFoo).asc(prop("z", "foo")).asc(prop("w", "foo")))
    indexOrder(interesting, properties, ASC) should be(ProvidedOrder.asc(xFoo).asc(yFoo))
  }

  // Label scan

  test("Label scan: Empty required order results in empty provided order") {
    providedOrderForLabelScan(InterestingOrder.empty, x) should be(ProvidedOrder.empty)
  }

  test("Label scan: Simple required order results in matching provided order") {
    providedOrderForLabelScan(requiredAscX, x) should be(ProvidedOrder.asc(x))
    providedOrderForLabelScan(requiredDescX, x) should be(ProvidedOrder.desc(x))
  }

  test("Label scan: Simple required order with projected variable results in matching provided order") {
    val interestingAsc = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("blob"), Map("blob" -> x)))
    providedOrderForLabelScan(interestingAsc, x) should be(ProvidedOrder.asc(x))

    val interestingDesc = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("blob"), Map("blob" -> x)))
    providedOrderForLabelScan(interestingDesc, x) should be(ProvidedOrder.desc(x))
  }

  test("Label scan: Multi variable required order results in matching provided order") {
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(x).asc(y))
    providedOrderForLabelScan(interestingOrder, x) should be(ProvidedOrder.asc(x))
  }

  test("Label scan: Multi variable required order results in empty provided order if variable order does not match") {
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(y).asc(x))
    providedOrderForLabelScan(interestingOrder, x) should be(ProvidedOrder.empty)
  }

  // Test the interesting part of the InterestingOrder

  test("Label scan: Single variable interesting order results in provided order when required can't be fulfilled or is empty") {
    providedOrderForLabelScan(InterestingOrder.interested(interestingAscX), x) should be(ProvidedOrder.asc(x))
    providedOrderForLabelScan(InterestingOrder.interested(interestingDescX), x) should be(ProvidedOrder.desc(x))

    providedOrderForLabelScan(requiredDescX.interesting(interestingAscY), y) should be(ProvidedOrder.asc(y))
    providedOrderForLabelScan(requiredAscX.interesting(interestingDescY), y) should be(ProvidedOrder.desc(y))
  }

  test("Label scan: Single variable interesting order with projection results in matching provided order") {
    val propAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("blob"), Map("blob" -> x)))
    providedOrderForLabelScan(propAsc, x) should be(ProvidedOrder.asc(x))

    val propDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("blob"), Map("blob" -> x)))
    providedOrderForLabelScan(propDesc, x) should be(ProvidedOrder.desc(x))
  }

  test("Label scan: results in empty provided order when neither required nor interesting can be fulfilled or are empty") {
    providedOrderForLabelScan(InterestingOrder.interested(interestingDescXFoo), x) should be(ProvidedOrder.empty)
    providedOrderForLabelScan(InterestingOrder.interested(interestingAscXFoo), x) should be(ProvidedOrder.empty)

    providedOrderForLabelScan(requiredDescXFoo.interesting(interestingDescXFoo), x) should be(ProvidedOrder.empty)
    providedOrderForLabelScan(requiredAscXFoo.interesting(interestingAscXFoo), x) should be(ProvidedOrder.empty)
  }

  test("extractVariableForValue") {
    extractVariableForValue(varFor("x"), Map.empty) should be(Some(varFor("x")))
    extractVariableForValue(prop("x", "prop"), Map.empty) should be(None)
    extractVariableForValue(prop("x", "prop"), Map("x" -> varFor("y"))) should be(None)
    extractVariableForValue(varFor("x"), Map("x" -> prop("y", "prop"))) should be(None)
    extractVariableForValue(varFor("x"), Map("x" -> varFor("z"),  "z" -> prop("y", "prop"))) should be(None)
    extractVariableForValue(varFor("x"), Map("x" -> varFor("z"))) should be(Some(varFor("z")))
  }

  private def indexOrder(interestingOrder: InterestingOrder, indexProperties: Seq[Property], orderCapability: IndexOrderCapability): ProvidedOrder =
    ResultOrdering.providedOrderForIndexOperator(interestingOrder, indexProperties, indexProperties.map(_ => CTInteger), _ => orderCapability)
}
