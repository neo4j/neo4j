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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{LengthFunction, PathImpl, Variable}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values.intValue

class LengthFunctionTest extends CypherFunSuite {

  test("length can be used on paths") {
    //given
    val p = PathImpl(mock[Node], mock[Relationship], mock[Node])
    val m = ExecutionContext.from("p" -> AnyValues.asPathValue(p))
    val lengthFunction = LengthFunction(Variable("p"))

    //when
    val result = lengthFunction.apply(m)(QueryStateHelper.empty)

    //then
    result should equal(intValue(1))
  }
  test("length can still be used on collections") {
    //given
    val l = Seq("it", "was", "the")
    val m = ExecutionContext.from("l" -> l)
    val lengthFunction = LengthFunction(Variable("l"))

    //when
    val result = lengthFunction.apply(m)(QueryStateHelper.empty)

    //then
    result should equal(intValue(3))
  }

  test("length can still be used on strings") {
    //given
    val s = "it was the"
    val m = ExecutionContext.from("s" -> s)
    val lengthFunction = LengthFunction(Variable("s"))

    //when
    val result = lengthFunction.apply(m)(QueryStateHelper.empty)

    //then
    result should equal(intValue(10))
  }
}
