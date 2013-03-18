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
package org.neo4j.cypher.internal.commands

import expressions.{ReduceFunction, Identifier, LengthFunction, Add, Literal}
import org.neo4j.cypher.internal.symbols._
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}

class ReduceTest extends Assertions {
  @Test def canReturnSomethingFromAnIterable() {
    val l = Seq("x", "xxx", "xx")
    val expression = Add(Identifier("acc"), LengthFunction(Identifier("n")))
    val collection = Identifier("l")
    val m = ExecutionContext.from("l" -> l)
    val s = QueryStateHelper.empty

    val reduce = ReduceFunction(collection, "n", expression, "acc", Literal(0))

    assert(reduce.apply(m)(s) === 6)
  }

  @Test def returns_null_from_null_collection() {
    val expression = Add(Identifier("acc"), LengthFunction(Identifier("n")))
    val collection = Literal(null)
    val m = ExecutionContext.empty
    val s = QueryStateHelper.empty

    val reduce = ReduceFunction(collection, "n", expression, "acc", Literal(0))

    assert(reduce(m)(s) === null)
  }

  @Test def reduce_has_the_expected_type_string() {
    val expression = Add(Identifier("acc"), Identifier("n"))
    val collection = Literal(Seq(1,2,3))

    val reduce = ReduceFunction(collection, "n", expression, "acc", Literal(""))
    val typ = reduce.calculateType(SymbolTable())

    assert(typ === StringType())
  }

  @Test def reduce_has_the_expected_type_number() {
    val expression = Add(Identifier("acc"), Identifier("n"))
    val collection = Literal(Seq(1,2,3))

    val reduce = ReduceFunction(collection, "n", expression, "acc", Literal(0))
    val typ = reduce.calculateType(SymbolTable())

    assert(typ === NumberType())
  }

  @Test def reduce_has_the_expected_type_array() { 
    val expression = Add(Identifier("acc"), Identifier("n"))
    val collection = Literal(Seq(1,2,3))

    val reduce = ReduceFunction(collection, "n", expression, "acc", Literal(Seq(1,2)))
    val typ = reduce.calculateType(new SymbolTable())

    assert(typ === new CollectionType(NumberType()))
  }
}
