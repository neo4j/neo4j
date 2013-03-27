/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.scalatest.Assertions
import org.neo4j.cypher.ExecutionEngineHelper
import org.junit.Test
import org.neo4j.cypher.internal.pipes.QueryStateHelper
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.ExecutionContext

class CreateNodeActionTest extends ExecutionEngineHelper with Assertions {

  @Test def mixed_types_are_not_ok() {
    val action = CreateNode("id", Map("*" -> Literal(Map("name" -> "Andres", "age" -> 37))), Seq.empty)

    val tx = graph.beginTx()
    action.exec(ExecutionContext.empty, QueryStateHelper.queryStateFrom(graph))
    tx.success()
    tx.finish()

    val n = graph.createdNodes.dequeue()

    assert(n.getProperty("name") === "Andres")
    assert(n.getProperty("age") === 37)
  }
}

