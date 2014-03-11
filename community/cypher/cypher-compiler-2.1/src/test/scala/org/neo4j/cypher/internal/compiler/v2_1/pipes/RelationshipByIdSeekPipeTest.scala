/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.spi.{Operations, QueryContext}
import org.neo4j.graphdb.Relationship
import org.mockito.Mockito
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Literal

class RelationshipByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek node by id") {
    // given
    val rel = mock[Relationship]
    val relOps = when(mock[Operations[Relationship]].getById(17)).thenReturn(rel).getMock[Operations[Relationship]]
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].relationshipOps).thenReturn(relOps).getMock[QueryContext]
    )

    // when
    val result = RelationshipByIdSeekPipe("a", Literal(17)).createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(rel))
  }
}
