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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, Multiply, Variable}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

class DistinctPipeTest extends CypherFunSuite {

  test("distinct input passes through") {
    //GIVEN
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 2)))

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    result.toList should beEquivalentTo(List(Map("x" -> 1), Map("x" -> 2)))
  }

  test("distinct executes expressions") {
    //GIVEN
    val expressions = Map("doubled" -> Multiply(Variable("x"), Literal(2)))
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 2)), expressions)

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    result.toList should beEquivalentTo(List(Map("doubled" -> 2), Map("doubled" -> 4)))
  }

  test("undistinct input passes through") {
    //GIVEN
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 1)))

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    result.toList should beEquivalentTo(List(Map("x" -> 1)))
  }

  test("distinct deals with maps containing java arrays") {
    //GIVEN
    val pipe = createDistinctPipe(List(
      Map("x" -> Map("prop" -> Array[String]("a", "b")).asJava),
      Map("x" -> Map("prop" -> Array[String]("a", "b")).asJava)))

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //THEN
    result should have size 1
    result.head("x").asInstanceOf[MapValue].get("prop") should equal(stringArray("a", "b"))
  }

  def createDistinctPipe(input: List[Map[String, Any]], expressions: Map[String, Expression] = Map("x" -> Variable("x"))) = {
    val source = new FakePipe(input, "x" -> CTNumber)
    DistinctPipe(source, expressions)()
  }
}
