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
package org.neo4j.internal.cypher.acceptance

import java.util
import org.neo4j.graphdb.Result

class ProcedureCallSupportAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap[String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001L)

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    ))
  }

  test("should return correctly typed list result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    ))
  }

  test("should return correctly typed stream result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")
    val stream = value.stream()

    registerProcedureReturningSingleValue(stream)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", stream)
    ))
  }

  val types: List[(AnyRef, Class[_])] = List(
    Byte.box(11.asInstanceOf[Byte]) -> classOf[java.lang.Long],
    Short.box(11.asInstanceOf[Short]) -> classOf[java.lang.Long],
    Int.box(11) -> classOf[java.lang.Long],
    Long.box(11L) -> classOf[java.lang.Long],
    Float.box(13.1F) -> classOf[java.lang.Double],
    Double.box(13.1D) -> classOf[java.lang.Double],
    Char.box('a') -> classOf[java.lang.String]
  )

  types.foreach {
    case (given, expected) =>
      test(s"${given.getClass.getSimpleName} should be ${expected.getSimpleName} in procedure") {
        registerProcedureReturningSingleValue(given)
        val result: Result = graph.execute("CALL my.first.value() YIELD out RETURN out")
        result.next().get("out").getClass should equal(expected)
        result.close()
      }
  }

  val arrayTypes: List[(AnyRef, Class[_])] = List(
    // Array -> Expected element type
    //-------------------------------
    // Primitive arrays
    Array[Byte](42.asInstanceOf[Byte]) -> classOf[java.lang.Long],
    Array[Short](42.asInstanceOf[Short]) -> classOf[java.lang.Long],
    Array[Int](42) -> classOf[java.lang.Long],
    Array[Long](42L) -> classOf[java.lang.Long],
    Array[Float](42.0F) -> classOf[java.lang.Double],
    Array[Double](42.0D) -> classOf[java.lang.Double],
    Array[Char]('a') -> classOf[java.lang.String],

    // Boxed arrays
    Array[java.lang.Byte](Byte.box(42.asInstanceOf[Byte])) -> classOf[java.lang.Long],
    Array[java.lang.Short](Short.box(42.asInstanceOf[Short])) -> classOf[java.lang.Long],
    Array[java.lang.Integer](Int.box(42)) -> classOf[java.lang.Long],
    Array[java.lang.Long](Long.box(42L)) -> classOf[java.lang.Long],
    Array[java.lang.Float](Float.box(42.0F)) -> classOf[java.lang.Double],
    Array[java.lang.Double](Double.box(42.0D)) -> classOf[java.lang.Double],
    Array[java.lang.Character](Char.box('a')) -> classOf[java.lang.String]
  )

  arrayTypes.foreach {
    case (given, expectedElementType) =>
      test(s"${given.getClass.getSimpleName} should have elements of type ${expectedElementType.getSimpleName} in procedure") {
        registerProcedureReturningSingleValue(given)
        val result = graph.execute("CALL my.first.value() YIELD out RETURN out")
        val resultValue = result.next().get("out")
        resultValue match {
          case r: Array[_] =>
            r.foreach(_.getClass should equal(expectedElementType))
        }

        result.close()
      }

  }
}
