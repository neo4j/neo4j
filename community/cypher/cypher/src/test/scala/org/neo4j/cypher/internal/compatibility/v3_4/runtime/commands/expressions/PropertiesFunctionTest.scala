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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import java.util

import org.mockito.Mockito
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.map

class PropertiesFunctionTest extends CypherFunSuite {

  import Mockito._

  val query = mock[QueryContext]
  val nodeOps = mock[Operations[Node]]
  val relOps = mock[Operations[Relationship]]

  when(query.nodeOps).thenReturn(nodeOps)
  when(query.relationshipOps).thenReturn(relOps)

  test("should return null if argument is null") {
    properties(null.asInstanceOf[AnyRef]) should be(NO_VALUE)
  }

  test("should map Java maps to maps") {
    val m = new util.HashMap[String, String]()
    m.put("a", "x")
    m.put("b", "y")

    properties(m) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))
  }

  test("should map Scala maps to maps") {
    val m = Map("a" -> "x", "b" -> "y")
    properties(m) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))

  }

  test("should map nodes to maps") {
    val node = mock[Node]
    when(node.getId).thenReturn(0)
    when(nodeOps.propertyKeyIds(0)).thenReturn(List(0,1).iterator)
    when(query.getPropertyKeyName(0)).thenReturn("a")
    when(query.getPropertyKeyName(1)).thenReturn("b")
    when(nodeOps.getProperty(0, 0)).thenReturn(stringValue("x"))
    when(nodeOps.getProperty(0, 1)).thenReturn(stringValue("y"))

    properties(node) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))
  }

  test("should map relationships to maps") {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(0)
    when(rel.getId).thenReturn(0)
    when(relOps.propertyKeyIds(0)).thenReturn(List(0,1).iterator)
    when(query.getPropertyKeyName(0)).thenReturn("a")
    when(query.getPropertyKeyName(1)).thenReturn("b")
    when(relOps.getProperty(0, 0)).thenReturn(stringValue("x"))
    when(relOps.getProperty(0, 1)).thenReturn(stringValue("y"))

    properties(rel) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))
  }

  test("should fail trying to map an int") {
    a[CypherTypeException] should be thrownBy {
      properties(12)
    }
  }

  test("should fail trying to map a string") {
    a[CypherTypeException] should be thrownBy {
      properties("Hullo")
    }
  }

  test("should fail trying to map a list") {
    a[CypherTypeException] should be thrownBy {
      properties(List.empty)
    }
  }

  private def properties(orig: Any) = {
    PropertiesFunction(Literal(orig))(ExecutionContext.empty, QueryStateHelper.empty.withQueryContext(query))
  }
}
