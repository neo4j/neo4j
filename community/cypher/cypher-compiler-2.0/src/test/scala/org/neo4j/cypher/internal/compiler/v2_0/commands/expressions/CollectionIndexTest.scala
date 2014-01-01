/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{SymbolTable, CollectionType, AnyType, FakeExpression}

class CollectionIndexTest extends Assertions {

  implicit val state = QueryStateHelper.empty
  val ctx = ExecutionContext.empty

  @Test def tests() {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    assert(idx(0) === 1)
    assert(idx(1) === 2)
    assert(idx(2) === 3)
    assert(idx(3) === 4)
    assert(idx(-1) === 4)
    assert(idx(100) === null)
  }

  @Test def empty_collection_tests() {
    implicit val collection = Collection()

    assert(idx(0) === null)
    assert(idx(-1) === null)
    assert(idx(100) === null)
  }

  @Test def shouldHandleNulls() {
    implicit val collection = Literal(null)

    assert(idx(0) === null)
  }

  @Test def typeWhenCollectionIsAnyTypeIsCollectionOfAnyType() {
    val collection = new FakeExpression(AnyType())
    val symbols = new SymbolTable()
    val result = CollectionIndex(collection, Literal(2)).evaluateType(CollectionType(AnyType()), symbols)

    assert(result === AnyType())
  }

  private def idx(value: Int)(implicit collection:Expression) =
    CollectionIndex(collection, Literal(value))(ctx)(state)
}
