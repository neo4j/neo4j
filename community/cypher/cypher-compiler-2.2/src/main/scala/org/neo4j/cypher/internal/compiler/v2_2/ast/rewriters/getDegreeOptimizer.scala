/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.graphdb.Direction

// Rewrites queries to allow using the much faster getDegree method on the nodes
case object getDegreeOptimizer extends Rewriter {
  def apply(that: AnyRef): AnyRef = bottomUp(instance)(that)

  val instance: Rewriter = Rewriter.lift {
    case func@FunctionInvocation(_, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(node), List(), None, _), RelationshipPattern(None, _, types, None, None, dir), NodePattern(None, List(), None, _))))))
      if func.function == Some(functions.Length) =>
        calculateUsingGetDegree(func, node, types, dir)

    case func@FunctionInvocation(_, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, List(), None, _), RelationshipPattern(None, _, types, None, None, dir), NodePattern(Some(node), List(), None, _))))))
      if func.function == Some(functions.Length) =>
      calculateUsingGetDegree(func, node, types, dir.reverse())
  }

  def calculateUsingGetDegree(func: FunctionInvocation, node: Identifier, types: Seq[RelTypeName], dir: Direction): Expression = {
    types
      .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(func.position))
      .getOrElse(GetDegree(node, None, dir)(func.position))
  }
}
