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

import org.neo4j.cypher.internal.compiler.v2_1.ast.{Statement, Query}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{PipeBuilder, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LogicalPlanContext, SimpleCostModel, GuessingEstimator, SimpleLogicalPlanner}
import org.neo4j.cypher.internal.compiler.v2_1.planner.execution.PipeExecutionPlanBuilder
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.ParsedQuery
import org.neo4j.cypher.internal.compiler.v2_1.Monitors

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class Planner(monitors: Monitors) extends PipeBuilder {
  val estimator = new GuessingEstimator
  val tokenResolver = new SimpleTokenResolver()
  val queryGraphBuilder = new SimpleQueryGraphBuilder
  val costs = new SimpleCostModel
  val executionPlanBuilder = new PipeExecutionPlanBuilder(monitors)
  val logicalPlanner = new SimpleLogicalPlanner()

  def producePlan(inputQuery: ParsedQuery, planContext: PlanContext): PipeInfo =
    producePlan(inputQuery.statement, inputQuery.semanticTable)(planContext)

  def producePlan(statement: Statement, semanticTable: SemanticTable)(planContext: PlanContext): PipeInfo = statement match {
    case ast: Query =>
      val resolvedAst = tokenResolver.resolve(ast)(planContext)
      val queryGraph = queryGraphBuilder.produce(resolvedAst)

      if(queryGraph.patternRelationships.size > 1)
        throw new CantHandleQueryException

      val context = LogicalPlanContext(planContext, estimator, costs, semanticTable, queryGraph)
      val logicalPlan = logicalPlanner.plan(context)
      executionPlanBuilder.build(logicalPlan)

    case _ =>
      throw new CantHandleQueryException
  }
}
