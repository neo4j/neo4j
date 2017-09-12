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
package org.neo4j.cypher.internal.compiler.v3_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.{NonEmpty, True}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.javacompat.ValueUtils.fromNodeProxy
import org.neo4j.values.storable.Values.{FALSE, TRUE}
import org.neo4j.values.virtual.PathValue

class PathExpressionTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("should accept shortest path expressions") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val pattern = ShortestPath(
      pathName = "p",
      left = SingleNode("a"),
      right = SingleNode("c"),
      relTypes = Seq(),
      dir = SemanticDirection.OUTGOING,
      allowZeroLength = false,
      maxDepth = None,
      single = true,
      relIterator = None)

    val expression = ShortestPathExpression(pattern)

    val m = ExecutionContext.from("a" -> a, "c" -> c)

    val result = withQueryState { state =>
      expression(m, state).asInstanceOf[PathValue]
    }

    result.startNode() should equal(fromNodeProxy(a))
    result.endNode() should equal(fromNodeProxy(c))
    result should have size 2
  }

  test("should handle expressions with labels") {
    // GIVEN
    val a = createNode()
    val b = createLabeledNode("Foo")

    relate(a, b)

    val pattern = RelatedTo(SingleNode("a"), SingleNode("  UNNAMED1", Seq(UnresolvedLabel("Foo"))), "  UNNAMED2", Seq.empty, SemanticDirection.OUTGOING, Map.empty)
    val expression = NonEmpty(PathExpression(Seq(pattern), True(), PathExtractorExpression(Seq(pattern))))
    val m = ExecutionContext.from("a" -> a)

    // WHEN
    val result = withQueryState { state =>
      expression(m, state)
    }

    // THEN
    result should equal(TRUE)
  }

  test("should return false if labels are missing") {
    // GIVEN
    val a = createNode()
    val b = createLabeledNode("Bar")

    relate(a, b)

    val pattern = RelatedTo(SingleNode("a"), SingleNode("  UNNAMED1", Seq(UnresolvedLabel("Foo"))), "  UNNAMED2", Seq.empty, SemanticDirection.OUTGOING, Map.empty)
    val expression = NonEmpty(PathExpression(Seq(pattern), True(), PathExtractorExpression(Seq(pattern))))
    val m = ExecutionContext.from("a" -> a)

    // WHEN
    val result = withQueryState { state =>
      expression(m, state)
    }

    // THEN
    result should equal(FALSE)
  }
}
