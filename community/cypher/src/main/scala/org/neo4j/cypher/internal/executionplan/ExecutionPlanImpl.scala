/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan

import builders._
import org.neo4j.graphdb._
import collection.Seq
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher._
import internal.commands._
import collection.mutable.{Map => MutableMap}
import internal.symbols.SymbolTable

class ExecutionPlanImpl(inputQuery: Query, graph: GraphDatabaseService) extends ExecutionPlan {
  val (executionPlan, executionPlanText) = prepareExecutionPlan()

  def execute(params: Map[String, Any]): ExecutionResult = executionPlan(params)

  private def prepareExecutionPlan(): ((Map[String, Any]) => ExecutionResult, String) = {

    var continue = true
    var planInProgress = ExecutionPlanInProgress(PartiallySolvedQuery(inputQuery), new ParameterPipe(), false)

    while (continue) {
      while (builders.exists(_.canWorkWith(planInProgress))) {
        val matchingBuilders = builders.filter(_.canWorkWith(planInProgress))

        val builder = matchingBuilders.sortBy(_.priority).head
        val newPlan = builder(planInProgress)

        if (planInProgress == newPlan) {
          throw new InternalException("Something went wrong trying to build your query. The offending builder was: " + builder.getClass.getSimpleName)
        }

        planInProgress = newPlan
      }

      if (!planInProgress.query.isSolved) {
        produceAndThrowException(planInProgress)
      }

      planInProgress.query.tail match {
        case None => continue = false
        case Some(q) => planInProgress = planInProgress.copy(query = q)
      }
    }

    val columns = getQueryResultColumns(inputQuery, planInProgress.pipe.symbols)
    val (pipe, func) = if (planInProgress.containsTransaction) {
      val p = new CommitPipe(planInProgress.pipe, graph)
      (p, getEagerReadWriteQuery(p, columns))
    } else {
      (planInProgress.pipe, getLazyReadonlyQuery(planInProgress.pipe, columns))
    }

    val executionPlan = pipe.executionPlan()

    (func, executionPlan)
  }

  private def getQueryResultColumns(q: Query, currentSymbols:SymbolTable) = {
    var query = q
    while (query.tail.isDefined) {
      query = query.tail.get
    }

    val columns = query.returns.columns.flatMap {
      case "*" => currentSymbols.identifiers.map(_.name).toList
      case x => Seq(x)
    }

    columns
  }

  private def getLazyReadonlyQuery(pipe: Pipe, columns: List[String]): Map[String, Any] => ExecutionResult = {
    val func = (params: Map[String, Any]) => {
      val state = new QueryState(graph, MutableMaps.create ++ params)
      new PipeExecutionResult(pipe.createResults(state), pipe.symbols, columns)
    }

    func
  }

  private def getEagerReadWriteQuery(pipe: Pipe, columns: List[String]): Map[String, Any] => ExecutionResult = {
    val func = (params: Map[String, Any]) => {
      val state = new QueryState(graph, MutableMaps.create ++ params)
      new EagerPipeExecutionResult(pipe.createResults(state), pipe.symbols, columns, state)
    }

    func
  }

  private def produceAndThrowException(plan: ExecutionPlanInProgress) {
    val errors = builders.flatMap(builder => builder.missingDependencies(plan).map(builder -> _)).toList.
      sortBy {
      case (builder, _) => builder.priority
    }

    if (errors.isEmpty) {
      throw new SyntaxException("""Somehow, Cypher was not able to construct a valid execution plan from your query.
The Neo4j team is very interested in knowing about this query. Please, consider sending a copy of it to cypher@neo4j.org.
Thank you!

The Neo4j Team
""")
    }

    val prio = errors.head._1.priority
    val errorsOfHighestPrio = errors.filter(_._1.priority == prio).map("Unknown identifier `" + _._2 + "`")

    val errorMessage = errorsOfHighestPrio.mkString("\n")
    throw new SyntaxException(errorMessage)
  }

  lazy val builders = Seq(
    new NodeByIdBuilder(graph),
    new IndexQueryBuilder(graph),
    new GraphGlobalStartBuilder(graph),
    new FilterBuilder,
    new NamedPathBuilder,
    new ExtractBuilder,
    //TODO: Reintroduce this when the problems with SlicePipe are solved.
    //new SortedAggregationBuilder,
    new MatchBuilder,
    new SortBuilder,
    new ColumnFilterBuilder,
    new SliceBuilder,
    new AggregationBuilder,
    new ShortestPathBuilder,
    new RelationshipByIdBuilder(graph),
    new CreateNodesAndRelationshipsBuilder(graph),
    new UpdateActionBuilder(graph),
    new EmptyResultBuilder
  )

  override def toString = executionPlanText
}