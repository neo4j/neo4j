/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeBuilder
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.execution.PipeExecutionPlanBuilder
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.{inSequence, bottomUp, ParsedQuery, Monitors}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class Planner(monitors: Monitors, metricsFactory: MetricsFactory, monitor: PlanningMonitor) extends PipeBuilder {
  val tokenResolver = new SimpleTokenResolver()
  val plannerQueryBuilder = new SimplePlannerQueryBuilder
  val executionPlanBuilder = new PipeExecutionPlanBuilder(monitors)
  val strategy = new GreedyPlanningStrategy()

  def producePlan(inputQuery: ParsedQuery, planContext: PlanContext): PipeInfo =
    producePlan(inputQuery.statement, inputQuery.semanticTable, inputQuery.queryText)(planContext)

  private def producePlan(statement: Statement, semanticTable: SemanticTable, query: String)(planContext: PlanContext): PipeInfo = {
    // TODO: When Ronja is the only planner around, move this to ASTRewriter
    val rewrittenStatement = rewriteStatement(statement)
    rewrittenStatement match {
      case ast: Query =>
        monitor.startedPlanning(query)
        val logicalPlan = produceLogicalPlan(ast, semanticTable)(planContext)
        monitor.foundPlan(query, logicalPlan)
        val result = executionPlanBuilder.build(logicalPlan)
        monitor.successfulPlanning(query, result)
        result

      case _ =>
        throw new CantHandleQueryException
    }
  }

  def rewriteStatement(statement: Statement) = {
    val namedStatement = statement.rewrite(bottomUp(
      inSequence(nameVarLengthRelationships, namePatternPredicates)
    )).asInstanceOf[Statement]

    val statementWithInlinedProjections = inlineProjections(namedStatement)

    statementWithInlinedProjections
  }

  def produceLogicalPlan(ast: Query, semanticTable: SemanticTable)(planContext: PlanContext): LogicalPlan = {
    tokenResolver.resolve(ast)(semanticTable, planContext)
    val (plannerQuery, subQueriesLookupTable) = plannerQueryBuilder.produce(ast)

    val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)
    val context = LogicalPlanContext(planContext, metrics, semanticTable, plannerQuery, subQueriesLookupTable, strategy)
    strategy.plan(context).plan
  }
}

trait PlanningMonitor {
  def startedPlanning(q: String)
  def foundPlan(q: String, p: LogicalPlan)
  def successfulPlanning(q: String, p: PipeInfo)
}
