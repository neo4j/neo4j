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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_0.mutation.MergePatternAction
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.PatternException
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.symbols.NodeType

/**
 * TODO: This whole class is wrong and should not exist. Move it to a semantic check on the AST when possible
 */
class MergeErrorReporter extends PlanBuilder with UpdateCommandExpander {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext) = {
    val unboundNodes = plan.query.updates.collect {
      case Unsolved(x: MergePatternAction) => x.identifiers.filter {
        case (id, typ) if typ.isInstanceOf[NodeType] => Identifier.isNamed(id)
        case _                                       => false
      }.map(_._1)
    }.flatten.mkString(", ")

    throw new PatternException("MERGE needs at least some part of the pattern to already be known. Please provide values for one of: " + unboundNodes)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    plan.query.updates.exists {
      case Unsolved(x: MergePatternAction) => true
      case _                               => false
    }

}
