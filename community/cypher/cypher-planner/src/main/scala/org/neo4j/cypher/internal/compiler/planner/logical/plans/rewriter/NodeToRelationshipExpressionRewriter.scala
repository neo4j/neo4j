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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions._
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewrite will replace all occurrences of the startNode and endNode in the expression with startNode(relationshipVariable) and endNode(relationshipVariable).
 * This is used to rewrite expressions in a few QPP based plan rewriters.
 * @param startNode - the node to replace with the startNode(rel)
 * @param endNode - the node to replace with the endNode(rel)
 * @param relationshipVariable - the relationship variable to use in the replacement.
 */
case class NodeToRelationshipExpressionRewriter(
  private val startNode: LogicalVariable,
  private val endNode: LogicalVariable,
  private val relationshipVariable: LogicalVariable
) extends Rewriter {

  private val innerRewriter: Rewriter = Rewriter.lift {
    case `startNode` => StartNode(relationshipVariable)(InputPosition.NONE)
    case `endNode`   => EndNode(relationshipVariable)(InputPosition.NONE)
    case AndedPropertyInequalities(v, _, inequalities) if v == startNode || v == endNode =>
      Ands.create(inequalities.map(_.endoRewrite(this)).toListSet)

    // cached properties require original variable names. If we do need to replace them we no longer can use the cache for it.
    case CachedProperty(_, entityVariable, _, _, _)
      if entityVariable == startNode || entityVariable == endNode =>
      throw new IllegalStateException(
        "We cannot rewrite cached property expressions to relationship expressions. Ensure this only used in rewriting phases before we InsertCacheProperties."
      )
    case CachedHasProperty(_, entityVariable, propertyKey, _, _)
      if entityVariable == startNode || entityVariable == endNode =>
      throw new IllegalStateException(
        "We cannot rewrite cached property expressions to relationship expressions. Ensure this only used in rewriting phases before we InsertCacheProperties."
      )
  }

  val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
