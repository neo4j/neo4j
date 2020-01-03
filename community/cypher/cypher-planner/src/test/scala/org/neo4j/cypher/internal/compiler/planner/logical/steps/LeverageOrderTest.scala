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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.ir.ProvidedOrder.{Asc, Desc}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class LeverageOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should leverage ASC order for exact match with grouping column") {
    val po = ProvidedOrder(Seq(Asc(varFor("a"))))
    val grouping = Map("newA" -> varFor("a"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }

  test("should leverage DESC order for exact match with grouping column") {
    val po = ProvidedOrder(Seq(Desc(varFor("a"))))
    val grouping = Map("newA" -> varFor("a"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }

  test("should leverage order for prefix match with grouping column") {
    val po = ProvidedOrder(Seq(Asc(varFor("a")), Desc(varFor("b"))))
    val grouping = Map("newA" -> varFor("a"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }

  test("should leverage order for exact match with one of grouping columns") {
    val po = ProvidedOrder(Seq(Asc(varFor("a"))))
    val grouping = Map("newA" -> varFor("a"), "newB" -> varFor("b"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }

  test("should leverage order for prefix match with one of grouping columns") {
    val po = ProvidedOrder(Seq(Asc(varFor("a")), Desc(varFor("b"))))
    val grouping = Map("newA" -> varFor("a"),  "newC" -> varFor("c"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }

  test("should leverage order for prefix match with one of grouping columns as prefix and one as suffix") {
    val po = ProvidedOrder(Seq(Asc(varFor("a")), Desc(varFor("b")), Asc(varFor("c"))))
    val grouping = Map("newA" -> varFor("a"), "newC" -> varFor("c"))
    leverageOrder(po, grouping) should be(Seq(varFor("a")))
  }
}
