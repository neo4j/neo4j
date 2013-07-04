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
package org.neo4j.cypher.internal.executionplan

import builders._
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher._
import internal.profiler.Profiler
import internal.spi.{PlanContext, QueryContext}
import internal.ClosingIterator
import internal.commands._
import internal.symbols.SymbolTable
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.ExecutionResult
import org.neo4j.cypher.internal.commands.values.{TokenType, KeyToken}
import org.neo4j.cypher.internal.commands.expressions.ExpressionResolver
import org.neo4j.cypher.internal.helpers.IsMap

class ExecutionPlanBuilder(graph: GraphDatabaseService) extends PatternGraphBuilder {

  type PipeAndIsUpdating = (Pipe, Boolean)

  def build(planContext: PlanContext, inputQuery: AbstractQuery): ExecutionPlan = {

    val (p, isUpdating) = buildPipes(planContext, inputQuery)

    val columns = getQueryResultColumns(inputQuery, p.symbols)
    val func = if (isUpdating) {
      getEagerReadWriteQuery(p, columns)
    } else {
      getLazyReadonlyQuery(p, columns)
    }

    new ExecutionPlan {
      def execute(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params, false)
      def profile(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params, true)
    }
  }

  def buildPipes(planContext: PlanContext, in: AbstractQuery): PipeAndIsUpdating = in match {
    case q: Query                     => buildQuery(q, planContext)
    case q: IndexOperation            => buildIndexQuery(q)
    case q: UniqueConstraintOperation => buildConstraintQuery(q)
    case q: Union                     => buildUnionQuery(q, planContext)
  }

  val unionBuilder = new UnionBuilder(this)

  def buildUnionQuery(union: Union, context:PlanContext): PipeAndIsUpdating = unionBuilder.buildUnionQuery(union, context)

  def buildIndexQuery(op: IndexOperation): PipeAndIsUpdating = (new IndexOperationPipe(op), true)

  def buildConstraintQuery(op: UniqueConstraintOperation): PipeAndIsUpdating = {
    val label = KeyToken.Unresolved(op.label, TokenType.Label)
    val propertyKey = KeyToken.Unresolved(op.propertyKey, TokenType.PropertyKey)

    (new ConstraintOperationPipe(op, label, propertyKey), true)
  }

  def buildQuery(inputQuery: Query, context: PlanContext): PipeAndIsUpdating = {
    val initialPSQ = PartiallySolvedQuery(inputQuery).rewrite(ExpressionResolver(context))

    var continue = true
    var planInProgress = ExecutionPlanInProgress(initialPSQ, NullPipe, isUpdating = false)

    while (continue) {
      while (builders.exists(_.canWorkWith(planInProgress, context))) {
        val matchingBuilders = builders.filter(_.canWorkWith(planInProgress, context))

        val builder = matchingBuilders.sortBy(_.priority).head
        val newPlan = builder(planInProgress, context)

        if (planInProgress == newPlan)
          throw new InternalException("Something went wrong trying to build your query. The offending builder was: "
            + builder.getClass.getSimpleName)

        planInProgress = newPlan
      }

      if (!planInProgress.query.isSolved) {
        produceAndThrowException(planInProgress)
      }

      planInProgress.query.tail match {
        case None => continue = false
        case Some(q) =>
          planInProgress = planInProgress.copy(query = q)
      }
    }

    (planInProgress.pipe, planInProgress.isUpdating)
  }

  private def getQueryResultColumns(q: AbstractQuery, currentSymbols: SymbolTable): List[String] = q match {
    case in: Query =>

      // Find the last query part
      var query = in
      while (query.tail.isDefined) {
        query = query.tail.get
      }

      query.returns.columns.flatMap {
        case "*" => currentSymbols.identifiers.keys
        case x   => Seq(x)
      }

    case union: Union => getQueryResultColumns(union.queries.head, currentSymbols)

    case _ => List.empty
  }


  private def getLazyReadonlyQuery(pipe: Pipe, columns: List[String]): (QueryContext, Map[String, Any], Boolean) => ExecutionResult = {
    val func = (queryContext: QueryContext, params: Map[String, Any], profile: Boolean) => {
      val (state, results, descriptor) = prepareStateAndResult(queryContext, params, pipe, profile)

      new PipeExecutionResult(results, columns, state, descriptor)
    }

    func
  }

  private def getEagerReadWriteQuery(pipe: Pipe, columns: List[String]): (QueryContext, Map[String, Any], Boolean) => ExecutionResult = {
    val func = (queryContext: QueryContext, params: Map[String, Any], profile: Boolean) => {
      val (state, results, descriptor) = prepareStateAndResult(queryContext, params, pipe, profile)
      new EagerPipeExecutionResult(results, columns, state, descriptor)
    }

    func
  }

  private def prepareStateAndResult(queryContext: QueryContext, params: Map[String, Any], pipe: Pipe, profile:Boolean):
    (QueryState, ClosingIterator, () => PlanDescription) = {

    try {
      val decorator = if (profile) new Profiler() else NullDecorator
      val state = new QueryState(graph, queryContext, params, decorator)
      val results: Iterator[collection.Map[String, Any]] = pipe.createResults(state)
      val closingIterator = new ClosingIterator(results, queryContext)
      val descriptor = () => decorator.decorate(pipe.executionPlanDescription, closingIterator.isEmpty)
      (state, closingIterator, descriptor)
    }
    catch {
      case (t: Throwable) =>
        queryContext.close(success = false)
        throw t
    }
  }

  private def produceAndThrowException(plan: ExecutionPlanInProgress) {
    val errors = builders.flatMap(builder => builder.missingDependencies(plan).map(builder -> _)).toList.
      sortBy {
      case (builder, _) => builder.priority
    }

    if (errors.isEmpty) {
      throw new SyntaxException( """Somehow, Cypher was not able to construct a valid execution plan from your query.
The Neo4j team is very interested in knowing about this query. Please, consider sending a copy of it to cypher@neo4j.org.
Thank you!

The Neo4j Team""")
    }

    val prio = errors.head._1.priority
    val errorsOfHighestPrio = errors.filter(_._1.priority == prio).distinct.map(_._2)

    val errorMessage = errorsOfHighestPrio.mkString("\n")
    throw new SyntaxException(errorMessage)
  }

  lazy val builders = Seq(
    new StartPointBuilder,
    new FilterBuilder,
    new NamedPathBuilder,
    new ExtractBuilder,
    new MatchBuilder,
    new SortBuilder,
    new ColumnFilterBuilder,
    new SliceBuilder,
    new AggregationBuilder,
    new ShortestPathBuilder,
    new CreateNodesAndRelationshipsBuilder(graph),
    new UpdateActionBuilder(graph),
    new EmptyResultBuilder,
    new TraversalMatcherBuilder,
    new TopPipeBuilder,
    new DistinctBuilder,
    new IndexLookupBuilder,
    new StartPointChoosingBuilder
  )
}