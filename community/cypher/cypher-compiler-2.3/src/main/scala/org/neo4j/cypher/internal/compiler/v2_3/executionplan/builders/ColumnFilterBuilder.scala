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

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, Pipe, ColumnFilterPipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, PartiallySolvedQuery, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.commands.{AllIdentifiers, ReturnItem, ReturnColumn}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

/**
 * This class should get rid of any extra columns built up while building the execution plan, that weren't in the
 * queries return clause.
 */
class ColumnFilterBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val p = plan.pipe

    val isLastPipe = q.tail.isEmpty

    if (!isLastPipe && q.returns == Seq(Unsolved(AllIdentifiers()))) {
      handleWithClause(q, plan)
    } else {
      handleReturnClause(q, p, plan)
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query

    val preConditionsMet = q.extracted &&
      !q.sort.exists(_.unsolved) &&
      !q.slice.exists(_.unsolved) &&
      (q.returns.forall(_.solved) && q.returns.nonEmpty)

    preConditionsMet && {
      val expectedColumns = getReturnItems(q.returns, plan.pipe.symbols).map (ri => ri.name).toSet
      val givenColumns = plan.pipe.symbols.keys.toSet

      expectedColumns != givenColumns
    }
  }

  private def handleReturnClause(q: PartiallySolvedQuery, inPipe: Pipe, plan: ExecutionPlanInProgress)
                                (implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val returnItems: Seq[ReturnItem] = getReturnItems(q.returns, inPipe.symbols)
    val outPipe = new ColumnFilterPipe(inPipe, returnItems)

    val resultQ = plan.query.copy(returns = q.returns.map(_.solve))

    plan.copy(pipe = outPipe, query = resultQ)
  }

  private def handleWithClause(q: PartiallySolvedQuery, plan: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    val resultQ = q.copy(returns = q.returns.map(_.solve))
    plan.copy(query = resultQ)
  }

  private def getReturnItems(q: Seq[QueryToken[ReturnColumn]], symbols: SymbolTable): Seq[ReturnItem] = q.map(_.token).flatMap {
    case x: ReturnItem     => Seq(x)
    case x: AllIdentifiers => x.expressions(symbols).map {
      case (n, e) => ReturnItem(e, n)
    }
  }
}
