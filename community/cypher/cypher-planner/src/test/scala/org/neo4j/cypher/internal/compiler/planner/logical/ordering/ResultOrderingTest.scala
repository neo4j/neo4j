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
import org.neo4j.cypher.internal.ir.{InterestingOrder, InterestingOrderCandidate, ProvidedOrder, RequiredOrderCandidate}
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.{ASC, BOTH, DESC, NONE}
import org.neo4j.cypher.internal.v4_0.expressions.Property
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class ResultOrderingTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val requiredAscXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
  private val requiredDescXFoo: InterestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")))
  private val requiredDescYFoo = InterestingOrder.required(RequiredOrderCandidate.desc(prop("y", "foo")))

  private val interestingAscXFoo: InterestingOrderCandidate = InterestingOrderCandidate.asc(prop("x", "foo"))
  private val interestingDescXFoo: InterestingOrderCandidate = InterestingOrderCandidate.desc(prop("x", "foo"))

  private val indexPropertyXFoo: Seq[Property] = Seq(prop("x", "foo"))

  test("Empty required order results in provided order of index order capability ascending") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
  }

  test("Single property required DESC still results in provided ASC if index is not capable of DESC") {
    indexOrder(requiredDescXFoo, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
  }

  test("Single property required ASC results in provided DESC if index is not capable of ASC") {
    indexOrder(requiredAscXFoo, indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property no capability results in empty provided order") {
    indexOrder(InterestingOrder.empty, indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(requiredDescYFoo, indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
  }

  test("Single property required order results in matching provided order for compatible index capability") {
    indexOrder(requiredAscXFoo, indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
    indexOrder(requiredDescXFoo, indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property required order with projected property results in matching provided order for compatible index capability") {
    val interestingAsc = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo"))))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(prop("x", "foo")))

    val interestingDesc = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo"))))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property required order with projected node results in matching provided order for compatible index capability") {
    val interestingAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo"), Map("y" -> varFor("x"))))
    indexOrder(interestingAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(prop("x", "foo")))

    val interestingDesc = InterestingOrder.required(RequiredOrderCandidate.desc(prop("y", "foo"), Map("y" -> varFor("x"))))
    indexOrder(interestingDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Multi property required order results in matching provided order for compatible index capability") {
    val properties = Seq("x", "y", "z").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")))
  }

  test("Multi property required order results in provided order if property order does not match") {
    val properties = Seq("y", "x", "z").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(prop("y", "foo")).asc(prop("x", "foo")).asc(prop("z", "foo")))
  }

  test("Multi property required order results in provided order if property order partially matches") {
    val properties = Seq("x", "z", "y").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("z", "foo")).asc(prop("y", "foo")))
  }

  test("Multi property required order results in provided order if mixed sort direction") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")).desc(prop("z", "foo")).asc(prop("w", "foo")))

    // Index can only give full ascending or descending, not a mixture. Therefore we follow the first required order
    indexOrder(interestingOrder, properties, BOTH) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("Shorter multi property required order results in provided order") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("Longer multi property required order results in partial matching provided order") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")).asc(prop("w", "foo")))
    indexOrder(interestingOrder, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))
  }

  // Test the interesting part of the InterestingOrder

  test("Single property interesting order results in provided order when required can't be fulfilled or is empty") {
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))

    indexOrder(requiredDescXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
    indexOrder(requiredAscXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property interesting order with projection results in matching provided order for compatible index capability") {
    val propAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo"))))
    indexOrder(propAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(prop("x", "foo")))

    val propDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo"))))
    indexOrder(propDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(prop("x", "foo")))


    val varAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo"), Map("y" -> varFor("x"))))
    indexOrder(varAsc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.asc(prop("x", "foo")))

    val varDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(prop("y", "foo"), Map("y" -> varFor("x"))))
    indexOrder(varDesc, indexPropertyXFoo, BOTH) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))

    indexOrder(requiredDescXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, ASC) should be(ProvidedOrder.asc(prop("x", "foo")))
    indexOrder(requiredAscXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, DESC) should be(ProvidedOrder.desc(prop("x", "foo")))
  }

  test("Single property empty provided order when there is no capability") {
    indexOrder(InterestingOrder.interested(interestingAscXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(InterestingOrder.interested(interestingDescXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)

    indexOrder(requiredDescXFoo.interesting(interestingAscXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
    indexOrder(requiredAscXFoo.interesting(interestingDescXFoo), indexPropertyXFoo, NONE) should be(ProvidedOrder.empty)
  }

  test("Multi property interesting order results in provided order when required can't be fulfilled or is empty") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    // can't fulfill first interesting so falls back on second interesting
    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(prop("y", "foo"))).interesting(interestingAscXFoo.asc(prop("y", "foo")))
    indexOrder(interestingDesc, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo.asc(prop("y", "foo"))).interesting(interestingDescXFoo.desc(prop("y", "foo")))
    indexOrder(interestingAsc, properties, DESC) should be(ProvidedOrder.desc(prop("x", "foo")).desc(prop("y", "foo")))

    // can't fulfill required so falls back on interesting
    val interestingDescRequired = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")).desc(prop("y", "foo"))).interesting(interestingAscXFoo.asc(prop("y", "foo")))
    indexOrder(interestingDescRequired, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))

    val interestingAscRequired = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo"))).interesting(interestingDescXFoo.desc(prop("y", "foo")))
    indexOrder(interestingAscRequired, properties, DESC) should be(ProvidedOrder.desc(prop("x", "foo")).desc(prop("y", "foo")))
  }

  test("Multi property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(prop("y", "foo"))).interesting(interestingDescXFoo.desc(prop("y", "foo")))
    indexOrder(interestingDesc, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo).interesting(InterestingOrderCandidate.asc(prop("y", "foo")))
    indexOrder(interestingAsc, properties, DESC) should be(ProvidedOrder.desc(prop("x", "foo")).desc(prop("y", "foo")))


    val interestingDescRequired = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")).desc(prop("y", "foo"))).interesting(interestingDescXFoo.desc(prop("y", "foo")))
    indexOrder(interestingDescRequired, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))

    val interestingAscRequired = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")).asc(prop("y", "foo"))).interesting(interestingAscXFoo.asc(prop("y", "foo")))
    indexOrder(interestingAscRequired, properties, DESC) should be(ProvidedOrder.desc(prop("x", "foo")).desc(prop("y", "foo")))
  }

  test("Multi property empty provided order when there is no capability") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interestingDesc = InterestingOrder.interested(interestingDescXFoo.desc(prop("y", "foo"))).interesting(interestingAscXFoo.asc(prop("y", "foo")))
    indexOrder(interestingDesc, properties, NONE) should be(ProvidedOrder.empty)

    val interestingAsc = InterestingOrder.interested(interestingAscXFoo.asc(prop("y", "foo"))).interesting(interestingDescXFoo.desc(prop("y", "foo")))
    indexOrder(interestingAsc, properties, NONE) should be(ProvidedOrder.empty)
  }

  test("Multi property interesting order results in provided order if mixed sort direction") {
    val properties = Seq("x", "y", "z").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingDescXFoo.asc(prop("y", "foo")).desc(prop("z", "foo")))
    indexOrder(interesting, properties, BOTH) should be(ProvidedOrder.desc(prop("x", "foo")).desc(prop("y", "foo")).desc(prop("z", "foo")))
  }

  test("Shorter multi property interesting order results in provided order") {
    val properties = Seq("x", "y", "z", "w").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingAscXFoo.asc(prop("y", "foo")))
    indexOrder(interesting, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")).asc(prop("z", "foo")).asc(prop("w", "foo")))
  }

  test("Longer multi property interesting order results in partial matching provided order") {
    val properties = Seq("x", "y").map { node => prop(node, "foo") }

    val interesting = InterestingOrder.interested(interestingAscXFoo.asc(prop("y", "foo")).asc(prop("z", "foo")).asc(prop("w", "foo")))
    indexOrder(interesting, properties, ASC) should be(ProvidedOrder.asc(prop("x", "foo")).asc(prop("y", "foo")))
  }

  private def indexOrder(interestingOrder: InterestingOrder, indexProperties: Seq[Property], orderCapability: IndexOrderCapability): ProvidedOrder =
    ResultOrdering.withIndexOrderCapability(interestingOrder, indexProperties, indexProperties.map(_ => CTInteger), _ => orderCapability)
}
