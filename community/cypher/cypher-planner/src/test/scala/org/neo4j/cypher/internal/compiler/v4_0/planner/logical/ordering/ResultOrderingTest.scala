/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.ordering

import org.neo4j.cypher.internal.ir.v4_0.{InterestingOrder, RequiredOrderCandidate, InterestingOrderCandidate, ProvidedOrder}
import org.neo4j.cypher.internal.planner.v4_0.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.v4_0.spi.IndexOrderCapability.{ASC, BOTH, DESC, NONE}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class ResultOrderingTest extends CypherFunSuite {

  test("Empty required order results in provided order of index order capability ascending") {
    val properties = Seq(("x", CTInteger))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.empty, properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
  }

  test("Single property required DESC still results in provided ASC if index is not capable of DESC") {
    val properties = Seq(("x", CTInteger))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x")), properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
  }

  test("Single property required ASC results in provided DESC if index is not capable of ASC") {
    val properties = Seq(("x", CTInteger))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x")), properties, capability(DESC)) should be(ProvidedOrder.desc("x"))
  }

  test("Single property no capability results in empty provided order") {
    val properties = Seq(("x", CTInteger))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.empty, properties, capability(NONE)) should be(ProvidedOrder.empty)
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("y")), properties, capability(NONE)) should be(ProvidedOrder.empty)
  }

  test("Single property required order results in matching provided order for compatible index capability") {
    val properties = Seq(("x", CTInteger))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x")), properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x")), properties, capability(DESC)) should be(ProvidedOrder.desc("x"))
  }

  test("Multi property required order results in matching provided order for compatible index capability") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y").asc("z"))

    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y").asc("z"))
  }

  test("Multi property required order results in provided order if property order does not match") {
    val properties = Seq(
      ("y", CTInteger),
      ("x", CTInteger),
      ("z", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y").asc("z"))

    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capability(ASC)) should be(ProvidedOrder.asc("y").asc("x").asc("z"))
  }

  test("Multi property required order results in provided order if property order partially matches") {
    val properties = Seq(
      ("x", CTInteger),
      ("z", CTInteger),
      ("y", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y").asc("z"))

    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("z").asc("y"))
  }

  test("Multi property required order results in provided order if mixed sort direction") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger),
      ("w", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y").desc("z").asc("w"))

    // Index can only give full ascending or descending, not a mixture. Therefore we follow the first required order
    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capability(BOTH)) should be(ProvidedOrder.asc("x").asc("y").asc("z").asc("w"))
  }

  test("Shorter multi property required order results in provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger),
      ("w", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y"))

    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y").asc("z").asc("w"))
  }

  test("Longer multi property required order results in partial matching provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y").asc("z").asc("w"))

    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => ASC
    ResultOrdering.withIndexOrderCapability(interestingOrder, properties, capabilities) should be(ProvidedOrder.asc("x").asc("y"))
  }

  // Test the interesting part of the InterestingOrder

  test("Single property interesting order results in provided order when required can't be fulfilled or is empty") {
    val properties = Seq(("x", CTInteger))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x"))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x")).interested(InterestingOrderCandidate.asc("x")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x")).interested(InterestingOrderCandidate.desc("x")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x"))
  }

  test("Single property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    val properties = Seq(("x", CTInteger))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x"))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x")).interested(InterestingOrderCandidate.desc("x")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x")).interested(InterestingOrderCandidate.asc("x")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x"))
  }

  test("Single property empty provided order when there is no capability") {
    val properties = Seq(("x", CTInteger))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)

    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x")).interested(InterestingOrderCandidate.asc("x")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x")).interested(InterestingOrderCandidate.desc("x")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)
  }

  test("Multi property interesting order results in provided order when required can't be fulfilled or is empty") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )

    // can't fulfill first interesting so falls back on second interesting
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x").desc("y")).interested(InterestingOrderCandidate.asc("x").asc("y")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x").asc("y")).interested(InterestingOrderCandidate.desc("x").desc("y")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x").desc("y"))

    // can't fulfill required so falls back on interesting
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x").desc("y")).interested(InterestingOrderCandidate.asc("x").asc("y")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y")).interested(InterestingOrderCandidate.desc("x").desc("y")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x").desc("y"))
  }

  test("Multi property capability results in default provided order when neither required nor interesting can be fulfilled or are empty") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )

    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.desc("x").desc("y")).interested(InterestingOrderCandidate.desc("x").desc("y")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.required(RequiredOrderCandidate.asc("x").asc("y")).interested(InterestingOrderCandidate.asc("x").asc("y")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x").desc("y"))

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x").desc("y")).interested(InterestingOrderCandidate.desc("x").desc("y")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y"))
    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x")).interested(InterestingOrderCandidate.asc("y")),
      properties, capability(DESC)) should be(ProvidedOrder.desc("x").desc("y"))
  }

  test("Multi property empty provided order when there is no capability") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x").desc("y")).interested(InterestingOrderCandidate.asc("x").asc("y")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x").asc("y")).interested(InterestingOrderCandidate.desc("x").desc("y")),
      properties, capability(NONE)) should be(ProvidedOrder.empty)
  }

  test("Multi property interesting order results in provided order if mixed sort direction") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger)
    )

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.desc("x").asc("y").desc("z")),
      properties, capability(BOTH)) should be(ProvidedOrder.desc("x").desc("y").desc("z"))
  }

  test("Shorter multi property interesting order results in provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger),
      ("w", CTInteger)
    )

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x").asc("y")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y").asc("z").asc("w"))
  }

  test("Longer multi property interesting order results in partial matching provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )

    ResultOrdering.withIndexOrderCapability(InterestingOrder.interested(InterestingOrderCandidate.asc("x").asc("y").asc("z").asc("w")),
      properties, capability(ASC)) should be(ProvidedOrder.asc("x").asc("y"))
  }

  private def capability(capability: IndexOrderCapability): Seq[CypherType] => IndexOrderCapability = _ => capability
}
