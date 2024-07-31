/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toNodeValue
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.ShortestPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.SingleNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.values.virtual.VirtualPathValue

class PathExpressionTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("should accept shortest path expressions") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val pattern = ShortestPath(
      pathName = "p",
      left = SingleNode("a", Some(Variable("a"))),
      right = SingleNode("c", Some(Variable("c"))),
      relTypes = Seq(),
      dir = SemanticDirection.OUTGOING,
      allowZeroLength = false,
      maxDepth = None,
      single = true,
      relIterator = None
    )

    val expression = ShortestPathExpression(pattern)

    val m = CypherRow.from("a" -> a, "c" -> c)

    val result = withQueryState() { state =>
      expression(m, state).asInstanceOf[VirtualPathValue]
    }

    result.startNodeId() should equal(a.id())
    result.endNodeId() should equal(c.id())
  }
}
