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
package org.neo4j.cypher.internal.compiler.v2_1.commands

import expressions._
import org.neo4j.cypher.internal.compiler.v2_1._
import pipes.QueryStateHelper
import symbols._
import org.scalatest.Assertions
import org.junit.Test

class ZipTest extends Assertions {
  @Test def canReturnSomethingFromAnIterable() {
    val l = Seq("x", "xxx", "xx")
    val l2 = Seq("x2", "xxx2", "xx2")
    val collection = Identifier("l")
    val collection2 = Identifier("l2")

    val m = ExecutionContext.from("l" -> l, "l2" -> l2)
    val s = QueryStateHelper.empty

    val zip = ZipFunction(collection, collection2)

    assert(zip.apply(m)(s) === Seq(Seq("x","x2"), Seq("xxx","xxx2"), Seq("xx", "xx2")))
  }

  @Test def returns_null_from_null_collection() {
    val collection = Literal(null)
    val collection2 = Literal(Seq("x"))
    val m = ExecutionContext.empty
    val s = QueryStateHelper.empty

    val zip = ZipFunction(collection, collection2)

    assert(zip(m)(s) === null)
  }

  @Test def returns_null_from_null_collection2() {
    val collection = Literal(Seq("x"))
    val collection2 = Literal(null)
    val m = ExecutionContext.empty
    val s = QueryStateHelper.empty

    val zip = ZipFunction(collection, collection2)

    assert(zip(m)(s) === null)
  }
}
