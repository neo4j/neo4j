/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CollectionSliceExpressionTest extends CypherFunSuite {

  test("tests") {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    slice(from = 0, to = 2) should equal(Seq(1, 2))
    slice(to = -2) should equal(Seq(1, 2))
    slice(from = 0, to = -1) should equal(Seq(1, 2, 3))
    slice(from = 2) should equal(Seq(3, 4))
    slice(from = -3) should equal(Seq(2, 3, 4))
    slice(to = 2) should equal(Seq(1, 2))
    slice(from = -3, to = -1) should equal(Seq(2, 3))
    sliceValue(null) should equal(null.asInstanceOf[Any])
  }

  test("should_handle_null") {
    implicit val collection = Literal(null)

    slice(from = -3, to = -1) should equal(null.asInstanceOf[Any])
  }

  test("should_handle_out_of_bounds_by_returning_null") {
    val fullSeq = Seq(1, 2, 3, 4)
    implicit val collection = Literal(fullSeq)

    slice(from = 2, to = 10) should equal(Seq(3,4))
    slice(to = -10) should equal(Seq.empty)
    slice(from = 5, to = -1) should equal(Seq.empty)
    slice(from = 5) should equal(Seq.empty)
    slice(from = -10) should equal(fullSeq)
    slice(to = 10) should equal(fullSeq)
    slice(from = -10, to = -1) should equal(Seq(1,2,3))
  }

  private val ctx = ExecutionContext.empty
  private implicit val state = QueryStateHelper.empty
  private val NO_VALUE = -666

  private def slice(from: Int = NO_VALUE, to: Int = NO_VALUE)(implicit collection: Expression) = {
    val f = if (from == NO_VALUE) None else Some(Literal(from))
    val t = if (to == NO_VALUE) None else Some(Literal(to))
    CollectionSliceExpression(collection, f, t)(ctx)(state)
  }

  private def sliceValue(in: Any) = {
    CollectionSliceExpression(Literal(in), Some(Literal(0)), Some(Literal(1)))(ctx)(state)
  }
}
