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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.{Pipe, ColumnFilterPipe}
import org.neo4j.cypher.internal.executionplan.{PlanBuilder, PartiallySolvedQuery, ExecutionPlanInProgress, LegacyPlanBuilder}
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.commands.{AllIdentifiers, ReturnItem, ReturnColumn}
import org.neo4j.cypher.internal.commands.expressions.Expression

class ColumnFilterBuilder extends LegacyPlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val isLastPipe = q.tail.isEmpty

    if (!isLastPipe && q.returns == Seq(Unsolved(AllIdentifiers()))) {
      handleWithClause(q, plan)
    } else {
      handleReturnClause(q, p, plan)
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query

    q.extracted &&
      !q.sort.exists(_.unsolved) &&
      !q.slice.exists(_.unsolved) &&
      q.returns.exists(_.unsolved)
  }

  def priority = PlanBuilder.ColumnFilter

  private def handleReturnClause(q: PartiallySolvedQuery, inPipe: Pipe, plan: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    val returnItems: Seq[ReturnItem] = getReturnItems(q.returns, inPipe.symbols)
    val expressionsToExtract: Map[String, Expression] = returnItems.map {
      case ReturnItem(expression, name, _) => name -> expression
    }.toMap

    val extractedStep = ExtractBuilder.extractIfNecessary(plan, expressionsToExtract)
    val filterPipe = new ColumnFilterPipe(extractedStep.pipe, returnItems)

    val after: SymbolTable = filterPipe.symbols
    val before: SymbolTable = inPipe.symbols

    val resultPipe = if (after != before) {
      filterPipe
    } else {
      inPipe
    }

    val resultQ = extractedStep.query.copy(returns = q.returns.map(_.solve))

    plan.copy(pipe = resultPipe, query = resultQ)
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