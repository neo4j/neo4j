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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_1.ast.NestedPlanExpression
import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.ast._

case object getDegreeRewriter extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = bottomUp(rewriter,
    // Do not traverse into NestedPlanExpressions as they have been optimized already by an earlier call to plan
    _.isInstanceOf[NestedPlanExpression])

  private def rewriter = Rewriter.lift {
    // LENGTH( (a)-[]->() )
    case func@FunctionInvocation(_, _, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(node), List(), None), RelationshipPattern(None, _, types, None, None, dir), NodePattern(None, List(), None))))))
      if func.function == functions.Length || func.function == functions.Size =>
      calculateUsingGetDegree(func, node, types, dir)

    // LENGTH( ()-[]->(a) )
    case func@FunctionInvocation(_, _, _, IndexedSeq(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, List(), None), RelationshipPattern(None, _, types, None, None, dir), NodePattern(Some(node), List(), None))))))
      if func.function == functions.Length || func.function == functions.Size =>
      calculateUsingGetDegree(func, node, types, dir.reversed)
  }

  private def calculateUsingGetDegree(func: FunctionInvocation, node: Variable, types: Seq[RelTypeName], dir: SemanticDirection): Expression = {
    types
      .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(func.position))
      .getOrElse(GetDegree(node, None, dir)(func.position))
  }
}
