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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.aux.v3_4.ParameterWrongTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.{Relationship, RelationshipType}
import org.neo4j.values.storable.Values.stringValue

class RelationshipTypeFunctionTest extends CypherFunSuite with FakeEntityTestSupport {

  private val mockedContext = mock[QueryContext]
  private val operations = mock[Operations[Relationship]]
  result(operations).when(mockedContext).relationshipOps

  private val state = QueryStateHelper.emptyWith(query = mockedContext)
  private val function = RelationshipTypeFunction(Variable("r"))

  test("should give the type of a relationship") {
    result(false).when(operations).isDeletedInThisTx(any())

    val rel = new FakeRel(null, null, RelationshipType.withName("T"))
    function.compute(rel, null, state) should equal(stringValue("T"))
  }

  test("should handle deleted relationships since types are inlined") {
    result(true).when(operations).isDeletedInThisTx(any())

    val rel = new FakeRel(null, null, RelationshipType.withName("T"))
    function.compute(rel, null, state) should equal(stringValue("T"))
  }

  test("should throw if encountering anything other than a relationship") {
    result(false).when(operations).isDeletedInThisTx(any())

    a [ParameterWrongTypeException] should be thrownBy function.compute(1337L, null, state)
  }

  private def result(value: Any) = doReturn(value, Nil: _*)
}
