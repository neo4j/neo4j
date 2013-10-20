/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cypher.OutOfBoundsException
import org.scalatest.Assertions
import org.junit.Test

class CollectionIndexTest extends Assertions {

  implicit val state = QueryStateHelper.empty
  val ctx = ExecutionContext.empty
  val collection = Literal(Seq(1, 2, 3, 4))

  @Test def tests() {
    assert(idx(0) === 1)
    assert(idx(1) === 2)
    assert(idx(2) === 3)
    assert(idx(3) === 4)
    assert(idx(-1) === 4)
    intercept[OutOfBoundsException](idx(4))
  }
  
  @Test def shouldHandleNulls() {
    val inValue = null
    val result = CollectionIndex(Literal(inValue), Literal(2))(ctx)(state)
    
    assert(result === null)
  }

  private def idx(value: Int) =
    CollectionIndex(collection, Literal(value))(ctx)(state)
}
