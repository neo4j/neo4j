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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ordering

import org.neo4j.cypher.internal.ir.v3_5.{AscColumnOrder, DescColumnOrder, RequiredOrder}
import org.neo4j.cypher.internal.planner.v3_5.spi.{AscIndexOrder, DescIndexOrder, IndexOrderCapability}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ResultOrderingTest extends CypherFunSuite {

  test("Empty required order satisfied by anything") {
    ResultOrdering.satisfiedWith(RequiredOrder.empty, ProvidedOrder.empty) should be(true)
    ResultOrdering.satisfiedWith(RequiredOrder.empty, ProvidedOrder(Seq(Ascending("x")))) should be(true)
    ResultOrdering.satisfiedWith(RequiredOrder.empty, ProvidedOrder(Seq(Descending("x")))) should be(true)
    ResultOrdering.satisfiedWith(RequiredOrder.empty, ProvidedOrder(Seq(Ascending("x"), Ascending("y")))) should be(true)
    ResultOrdering.satisfiedWith(RequiredOrder.empty, ProvidedOrder(Seq(Descending("x"), Descending("y")))) should be(true)
  }

  test("Single property required order satisfied by matching provided order") {
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Ascending("x")))) should be(true)
  }

  test("Single property required order satisfied by longer provided order") {
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Ascending("x"), Ascending("y")))) should be(true)
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Ascending("x"), Descending("y")))) should be(true)
  }

  test("Single property required order not satisfied by mismatching provided order") {
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Ascending("y")))) should be(false)
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Descending("x")))) should be(false)
    ResultOrdering.satisfiedWith(RequiredOrder(Seq(("x", AscColumnOrder))), ProvidedOrder(Seq(Ascending("y"), Ascending("x")))) should be(false)
  }

  test("Multi property required order satisfied only be matching provided order") {
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", DescColumnOrder),
      ("z", AscColumnOrder)
    ))
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x")))) should be(false)
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x"), Descending("y")))) should be(false)
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x"), Descending("y"), Ascending("z")))) should be(true)
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x"), Descending("z"), Ascending("y")))) should be(false)
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x"), Descending("y"), Descending("z")))) should be(false)
    ResultOrdering.satisfiedWith(requiredOrder, ProvidedOrder(Seq(Ascending("x"), Ascending("y"), Descending("z")))) should be(false)
  }

  test("Empty required order results in provided order of index order capability ascending") {
    val properties = Seq(("x", CTInteger))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(RequiredOrder.empty, properties, capabilities) should be(ProvidedOrder(Seq(Ascending("x"))))
  }

  test("Single property required order results in exception if index capability is descending") {
    val properties = Seq(("x", CTInteger))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => DescIndexOrder
    withClue("Throw exception for descending index with ascending required order") {
      an[IllegalStateException] should be thrownBy {
        ResultOrdering.withIndexOrderCapability(RequiredOrder(Seq(("x", AscColumnOrder))), properties, capabilities)
      }
    }
    withClue("Throw exception for descending index even with descending required order (not yet supported)") {
      an[IllegalStateException] should be thrownBy {
        ResultOrdering.withIndexOrderCapability(RequiredOrder(Seq(("x", DescColumnOrder))), properties, capabilities)
      }
    }
  }

  test("Single property required order results in matching provided order for compatible index capability") {
    val properties = Seq(("x", CTInteger))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(RequiredOrder(Seq(("x", AscColumnOrder))), properties, capabilities) should be(ProvidedOrder(Seq(Ascending("x"))))
    // Index can't give descending. Therefore we take what we have, which is ascending
    ResultOrdering.withIndexOrderCapability(RequiredOrder(Seq(("x", DescColumnOrder))), properties, capabilities) should be(ProvidedOrder(Seq(Ascending("x"))))
  }

  test("Multi property required order results in matching provided order for compatible index capability") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder),
      ("z", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(Seq(Ascending("x"), Ascending("y"), Ascending("z"))))
  }

  test("Multi property required order results in provided order if property order does not match") {
    val properties = Seq(
      ("y", CTInteger),
      ("x", CTInteger),
      ("z", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder),
      ("z", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(List(Ascending("y"), Ascending("x"), Ascending("z"))))
  }

  test("Multi property required order results in provided order if property order partially matches") {
    val properties = Seq(
      ("x", CTInteger),
      ("z", CTInteger),
      ("y", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder),
      ("z", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(List(Ascending("x"), Ascending("z"), Ascending("y"))))
  }

  test("Multi property required order results in  provided order if mixed sort direction") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger),
      ("w", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder),
      ("z", DescColumnOrder),
      ("w", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    // Index can't give descending. Therefore we take what we have, which is ascending
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(List(Ascending("x"), Ascending("y"), Ascending("z"), Ascending("w"))))
  }

  test("Shorter multi property required order results in provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger),
      ("z", CTInteger),
      ("w", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(List(Ascending("x"), Ascending("y"), Ascending("z"), Ascending("w"))))
  }

  test("Longer multi property required order results in partial matching provided order") {
    val properties = Seq(
      ("x", CTInteger),
      ("y", CTInteger)
    )
    val requiredOrder = RequiredOrder(Seq(
      ("x", AscColumnOrder),
      ("y", AscColumnOrder),
      ("z", AscColumnOrder),
      ("w", AscColumnOrder)
    ))
    val capabilities: Seq[CypherType] => IndexOrderCapability = _ => AscIndexOrder
    ResultOrdering.withIndexOrderCapability(requiredOrder, properties, capabilities) should be(ProvidedOrder(Seq(Ascending("x"), Ascending("y"))))
  }

}
