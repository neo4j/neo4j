/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2

import org.mockito.Matchers.any
import org.mockito.Mockito.doReturn
import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expression, Literal, Property}
import org.neo4j.cypher.internal.compiler.v3_2.commands.values.KeyToken.Unresolved
import org.neo4j.cypher.internal.compiler.v3_2.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v3_2.mutation.PropertySetAction
import org.neo4j.graphdb.{Node, Relationship}

class PropertySetActionTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("should work when the node is identified by a general expression and not simply by a variable") {
    createNode()

    val nodeExpr: Expression = mock[Expression]
    doReturn(mock[Node]).when(nodeExpr).apply(any())(any())
    val action = PropertySetAction(Property(nodeExpr, Unresolved("name", PropertyKey)), Literal("neo4j"))
    withCountsQueryState { queryState =>
      action.exec(ExecutionContext.empty, queryState)
      queryState.getStatistics.propertiesSet should equal(1)
    }
  }

  test("should work when the relationship is identified by a general expression and not simply by a variable") {
    relate(createNode(), createNode())

    val nodeExpr: Expression = mock[Expression]
    doReturn(mock[Relationship]).when(nodeExpr).apply(any())(any())
    val action = PropertySetAction(Property(nodeExpr, Unresolved("name", PropertyKey)), Literal("neo4j"))
    withCountsQueryState { queryState =>
      action.exec(ExecutionContext.empty, queryState)
      queryState.getStatistics.propertiesSet should equal(1)
    }
  }
}
