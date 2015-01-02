/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryStateHelper
import org.junit.Test
import org.scalatest.Assertions

class CollectionSliceExpressionTest extends Assertions {

  @Test def tests() {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    assert(slice(from = 0, to = 2) === Seq(1, 2), "[1,2,3,4][0..2]")

    assert(slice(to = -2) === Seq(1, 2), "[1,2,3,4][..-2]")

    assert(slice(from = 0, to = -1) === Seq(1, 2, 3), "[1,2,3,4][0..-1]")

    assert(slice(from = 2) === Seq(3, 4), "[1,2,3,4][2..]")

    assert(slice(from = -3) === Seq(2, 3, 4), "[1,2,3,4][-3..]")

    assert(slice(to = 2) === Seq(1, 2), "[1,2,3,4][..2]")

    assert(slice(from = -3, to = -1) === Seq(2, 3), "[1,2,3,4][-3..-1]")

    assert(sliceValue(null) === null)
  }

  @Test def should_handle_null() {
    implicit val collection = Literal(null)

    assert(slice(from = -3, to = -1) === null, "null[-3..-1]")
  }
  
  @Test def should_handle_out_of_bounds_by_returning_null() {
    val fullSeq = Seq(1, 2, 3, 4)
    implicit val collection = Literal(fullSeq)
    
    assert(slice(from = 2, to = 10) === Seq(3,4), "[1,2,3,4][2..10]")

    assert(slice(to = -10) === Seq.empty, "[1,2,3,4][..-10]")

    assert(slice(from = 5, to = -1) === Seq.empty, "[1,2,3,4][5..-1]")

    assert(slice(from = 5) === Seq.empty, "[1,2,3,4][5..]")

    assert(slice(from = -10) === fullSeq, "[1,2,3,4][-10..]")

    assert(slice(to = 10) === fullSeq, "[1,2,3,4][..10]")

    assert(slice(from = -10, to = -1) === Seq(1,2,3), "[1,2,3,4][-3..-1]")
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
