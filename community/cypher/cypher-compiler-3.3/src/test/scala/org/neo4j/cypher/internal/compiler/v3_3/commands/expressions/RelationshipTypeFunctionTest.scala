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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v3_3.spi.Operations
import org.neo4j.cypher.internal.frontend.v3_3.ParameterWrongTypeException
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Relationship, RelationshipType}

class RelationshipTypeFunctionTest extends CypherFunSuite with FakeEntityTestSupport {

  private val mockedContext = mock[QueryContext]
  private val operations = mock[Operations[Relationship]]
  doReturn(operations).when(mockedContext).relationshipOps

  private implicit val state = QueryStateHelper.emptyWith(query = mockedContext)

  test("should give the type of a relationship") {
    doReturn(false).when(operations).isDeletedInThisTx(any())

    val rel = new FakeRel(null, null, RelationshipType.withName("T"))
    RelationshipTypeFunction(Variable("r")).compute(rel, null) should equal("T")
  }

  test("should handle deleted relationships since types are inlined") {
    doReturn(true).when(operations).isDeletedInThisTx(any())

    val rel = new FakeRel(null, null, RelationshipType.withName("T"))
    RelationshipTypeFunction(Variable("r")).compute(rel, null) should equal("T")
  }

  test("should throw if encountering anything other than a relationship") {
    doReturn(false).when(operations).isDeletedInThisTx(any())

    a [ParameterWrongTypeException] should be thrownBy RelationshipTypeFunction(Variable("r")).compute((), null)
  }
}
