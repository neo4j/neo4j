/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.RelatedTo
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.MergePatternAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{MergeIntoPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

class MergeIntoBuilder extends PlanBuilder {
  override def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.updates.exists {
      case Unsolved(merge@MergePatternAction(_, _, _, _, _, _)) => canWorkWith(merge, plan.pipe.symbols)
      case _ => false
    }

  override def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val (relatedTo, queryToken, merge)= plan.query.updates.collectFirst {
      case token@Unsolved(merge@MergePatternAction(_, _, _, _, _, _)) if canWorkWith(merge, plan.pipe.symbols) =>
        (merge.patterns.head.asInstanceOf[RelatedTo], token, merge)
    }.get

    val props: Map[KeyToken, Expression] = relatedTo.properties.map {
      case (key, value) => resolve(ctx)(key) -> value
    }.toMap

    val mergePipe = MergeIntoPipe(plan.pipe, relatedTo.left.name,
      relatedTo.relName, relatedTo.right.name, relatedTo.direction,
      relatedTo.relTypes.head, props, merge.onCreate, merge.onMatch)()

    //mark as solved
    val newUpdates = plan.query.updates.replace(queryToken, queryToken.solve)

    plan.copy(
      pipe = mergePipe,
      query = plan.query.copy(updates = newUpdates),
      isUpdating = true
    )
  }

  private def resolve(ctx: PlanContext)(key: String) = UnresolvedProperty(key).resolve(ctx)

  private def canWorkWith(m: MergePatternAction, s: SymbolTable): Boolean =
    m.patterns.size == 1 &&
      m.patterns.forall {
        case r: RelatedTo => s.hasIdentifierNamed(r.left.name) && s.hasIdentifierNamed(r.right.name)
        case _ => false
      }
}
