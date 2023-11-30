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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.AbstractProcedureCallProjection
import org.neo4j.cypher.internal.ir.QueryHorizon

case class ProcedureCallProjection(call: ResolvedCall) extends AbstractProcedureCallProjection {

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] =
    coveredIds ++ call.callResults.map { result =>
      result.variable
    }

  override def dependingExpressions: Seq[Expression] = call.callArguments

  override def readOnly: Boolean = call.containsNoUpdates

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this

  override def isTerminatingProjection: Boolean = false
}
