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

class ExecutionPlanImpl(inputQuery: Query, graph: GraphDatabaseService) extends ExecutionPlan {
  val (executionPlan, executionPlanText) = prepareExecutionPlan()

  def execute(params: Map[String, Any]): ExecutionResult = executionPlan(params)

  private def prepareExecutionPlan(): ((Map[String, Any]) => PipeExecutionResult, String) = {
    var pipe: Pipe = new ParameterPipe()
    var query = PartiallySolvedQuery(inputQuery)

    while (builders.exists(_.isDefinedAt(pipe, query))) {
      val matchingBuilders = builders.filter(_.isDefinedAt(pipe, query))

      val builder = matchingBuilders.sortBy(_.priority).head
      val (p, q) = builder(pipe, query)

      if(p==pipe && q == query) {
        throw new InternalException("Something went wrong trying to build your query. The offending builder was: " + builder.getClass.getSimpleName)
      }
      
      pipe = p
      query = q
    }

    if (!query.isSolved) {
      val sp = new ShortestPathBuilder
      sp.checkForUnsolvedShortestPaths(query, pipe)

      checkForMissingPredicates(query, pipe)
    }

    val func = (params: Map[String, Any]) => {
      val newMap = MutableMap() ++ params
      new PipeExecutionResult(pipe.createResults(newMap), pipe.symbols, inputQuery.returns.columns)
    }
    val executionPlan = pipe.executionPlan()

    (func, executionPlan)
  }

  private def checkForMissingPredicates(querySoFar: PartiallySolvedQuery, pipe: Pipe) {
    val unsolvedPredicates = querySoFar.where.filter(_.unsolved).map(_.token)
    if (unsolvedPredicates.nonEmpty) {
      val x = unsolvedPredicates.
        flatMap(pred => pipe.symbols.missingDependencies(pred.dependencies)).
        map(_.name).
        mkString(",")

      throw new SyntaxException("Unknown identifier `" + x + "`.")
    }
  }

  lazy val builders = Seq(
    new NodeByIdBuilder(graph),
    new IndexQueryBuilder(graph),
    new GraphGlobalStartBuilder(graph),
    new FilterBuilder,
    new NamedPathBuilder,
    new ExtractBuilder,
    new SortedAggregationBuilder,
    new MatchBuilder,
    new SortBuilder,
    new ColumnFilterBuilder,
    new SliceBuilder,
    new AggregationBuilder,
    new ShortestPathBuilder,
    new RelationshipByIdBuilder(graph))

  override def toString = executionPlanText
}