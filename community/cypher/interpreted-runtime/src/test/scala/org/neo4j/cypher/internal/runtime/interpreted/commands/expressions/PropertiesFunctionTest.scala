/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import java.util

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.opencypher.v9_0.util.CypherTypeException
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.map
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

class PropertiesFunctionTest extends CypherFunSuite {

  import Mockito._

  val query = mock[QueryContext]
  val nodeOps = mock[Operations[NodeValue]]
  val relOps = mock[Operations[RelationshipValue]]

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
    when(nodeOps.propertyKeyIds(any())).thenAnswer(new Answer[Iterator[Int]] {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[Int] = List(0,1).iterator
    })
    when(query.getPropertyKeyName(0)).thenReturn("a")
    when(query.getPropertyKeyName(1)).thenReturn("b")
    when(nodeOps.getProperty(0, 0)).thenReturn(stringValue("x"))
    when(nodeOps.getProperty(0, 1)).thenReturn(stringValue("y"))
    when(query.getOptPropertyKeyId("a")).thenReturn(Some(0))
    when(query.getOptPropertyKeyId("b")).thenReturn(Some(1))

    properties(node) should equal(map(Array("a", "b"), Array(stringValue("x"), stringValue("y"))))
  }

  test("should map relationships to maps") {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(0)
    when(rel.getId).thenReturn(0)
    when(relOps.propertyKeyIds(any())).thenAnswer(new Answer[Iterator[Int]] {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[Int] = List(0,1).iterator
    })
    when(query.getPropertyKeyName(0)).thenReturn("a")
    when(query.getPropertyKeyName(1)).thenReturn("b")
    when(relOps.getProperty(0, 0)).thenReturn(stringValue("x"))
    when(relOps.getProperty(0, 1)).thenReturn(stringValue("y"))
    when(query.getOptPropertyKeyId("a")).thenReturn(Some(0))
    when(query.getOptPropertyKeyId("b")).thenReturn(Some(1))

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
