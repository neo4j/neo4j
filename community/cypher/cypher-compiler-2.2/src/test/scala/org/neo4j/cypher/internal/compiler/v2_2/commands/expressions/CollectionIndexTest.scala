/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class CollectionIndexTest extends CypherFunSuite {

  implicit val state = QueryStateHelper.empty
  val ctx = ExecutionContext.empty
  val expectedNull: Any = null

  test("tests") {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    idx(0) should equal(1)
    idx(1) should equal(2)
    idx(2) should equal(3)
    idx(3) should equal(4)
    idx(-1) should equal(4)
    idx(100) should equal(expectedNull)
  }

  test("empty_collection_tests") {
    implicit val collection = Collection()

    idx(0) should equal(expectedNull)
    idx(-1) should equal(expectedNull)
    idx(100) should equal(expectedNull)
  }

  test("shouldHandleNulls") {
    implicit val collection = Literal(null)

    idx(0) should equal(expectedNull)
  }

  test("typeWhenCollectionIsAnyTypeIsCollectionOfCTAny") {
    val collection = new FakeExpression(CTAny)
    val symbols = new SymbolTable()
    val result = CollectionIndex(collection, Literal(2)).evaluateType(CTCollection(CTAny), symbols)

    result should equal(CTAny)
  }

  private def idx(value: Int)(implicit collection:Expression) =
    CollectionIndex(collection, Literal(value))(ctx)(state)
}
