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

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, ExtractPipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, CachedExpression, Expression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.AllIdentifiers
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

/**
 * This builder will materialize expression results down to the result map, so they can be seen by the user
 */
class ExtractBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query

    // This is just a query part switch. No need to extract anything
    if (q.tail.nonEmpty && q.returns == Seq(Unsolved(AllIdentifiers()))) {
      plan.copy(query = q.copy(returns = Seq(Solved(AllIdentifiers()))))
    } else {
      val expressions: Map[String, Expression] =
        q.returns.flatMap(_.token.expressions(plan.pipe.symbols)).toMap

      val result = ExtractBuilder.extractIfNecessary(plan, expressions, materializeAll = true)
      result.copy(query = result.query.copy(returns = result.query.returns.map(_.solve)))
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val unsolvedReturnItems = q.returns.filter(_.unsolved)

    val a = unsolvedReturnItems.forall {
      ri => ri.token.expressions(plan.pipe.symbols).values.forall {
        e => e.symbolDependenciesMet(plan.pipe.symbols)
      }
    }

    val b = q.readyToAggregate
    val c = !q.aggregateToDo
    a && b && c && unsolvedReturnItems.nonEmpty
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] =
    if (plan.query.patterns.exists(_.unsolved))
      Seq.empty
    else {
      val unsolvedReturnItems = plan.query.returns.filter(_.unsolved)

      unsolvedReturnItems.flatMap {
        ri => ri.token.expressions(plan.pipe.symbols).values.flatMap {
          e => e.symbolTableDependencies
        }
      }.distinct.
        map("Unknown identifier `%s`.".format(_))
    }
}

object ExtractBuilder {
  /**
   *
   * @param plan the plan to work on
   * @param expressionsToExtract extract these expressions
   * @param materializeAll materialise results even for non-deterministic calls
   * @return
   */
  def extractIfNecessary(plan: ExecutionPlanInProgress, expressionsToExtract: Map[String, Expression], materializeAll:Boolean = false)
                        (implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {

    val expressions = expressionsToExtract.filter {
      case (k, CachedExpression(_, _))      => false
      case (k1, Identifier(k2)) if k1 == k2 => false
      case _                                => true
    }

    val query = plan.query
    val pipe = plan.pipe

    if (expressions.nonEmpty) {
      val newPsq = expressions.foldLeft(query)((psq, entry) => psq.rewrite(fromQueryExpression => {
        val (key, expr) = entry
        val correctExpression  = expr == fromQueryExpression
        val shouldMaterialize = expr.isDeterministic || materializeAll
        if (correctExpression && shouldMaterialize)
          CachedExpression(key, fromQueryExpression.getType(plan.pipe.symbols))
        else
          fromQueryExpression
      }))

      val resultPipe = ExtractPipe(pipe, expressions)
      val resultQuery = newPsq.copy(extracted = true)
      plan.copy(pipe = resultPipe, query = resultQuery)
    } else {
      plan.copy(query = query.copy(extracted = true))
    }
  }
}
