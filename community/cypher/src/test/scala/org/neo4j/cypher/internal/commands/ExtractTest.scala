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

import expressions.{ExtractFunction, Identifier, LengthFunction}
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}

class ExtractTest extends Assertions {
  @Test def canReturnSomethingFromAnIterable() {
    val l = Seq("x", "xxx", "xx")
    val expression = LengthFunction(Identifier("n"))
    val collection = Identifier("l")
    val m = ExecutionContext.from("l" -> l)

    val extract = ExtractFunction(collection, "n", expression)

    assert(extract.apply(m)(QueryStateHelper.empty) === Seq(1, 3, 2))
  }
}