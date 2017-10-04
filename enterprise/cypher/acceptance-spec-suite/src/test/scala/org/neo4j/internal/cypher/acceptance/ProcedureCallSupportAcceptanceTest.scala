/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.helpers.collection.MapUtil.map
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.proc.{Context, FieldSignature, Neo4jTypes}

class ProcedureCallSupportAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap[String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001)

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
      java.util.Collections.singletonMap("out", value)
    ))
  }

  test("should not yield deprecated fields") {
    // given
    registerProcedure("something.with.deprecated.output") { builder =>
      builder.out(util.Arrays.asList(
        FieldSignature.outputField("one",Neo4jTypes.NTString),
        FieldSignature.outputField("oldTwo",Neo4jTypes.NTString, true),
        FieldSignature.outputField("newTwo",Neo4jTypes.NTString) ))
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef]):  RawIterator[Array[AnyRef], ProcedureException] =
          RawIterator.of[Array[AnyRef], ProcedureException](Array("alpha","junk","beta"))
      }
    }

    // then
    graph.execute("CALL something.with.deprecated.output()").stream().toArray.toList should equal(List(
      map("one", "alpha", "newTwo", "beta")
    ))
  }

  test("should be able to execute union of multiple token accessing procedures") {
    val a = createLabeledNode("Foo")
    val b = createLabeledNode("Bar")
    relate(a, b, "REL", Map("prop" -> 1))

    val query =
      "CALL db.labels() YIELD label RETURN label as result " +
      "UNION " +
      "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType as result " +
      "UNION " +
      "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey as result " +
      "UNION " +
      "CALL db.labels() YIELD label RETURN label as result"

    val result = graph.execute(query)

    result.columnAs[String]("result").stream().toArray.toList shouldEqual List(
      "Foo",
      "Bar",
      "REL",
      "prop"
    )
  }
}
