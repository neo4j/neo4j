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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan

import builders._
import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.values.{TokenType, KeyToken}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.prepare.{AggregationPreparationRewriter, KeyTokenResolver}
import pipes._
import profiler.Profiler
import symbols.SymbolTable
import org.neo4j.cypher.{SyntaxException, ExecutionResult}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_0.spi.{QueryContext, PlanContext}

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
    val initialPSQ = PartiallySolvedQuery(inputQuery)

    var continue = true
    var planInProgress = ExecutionPlanInProgress(initialPSQ, NullPipe(), isUpdating = false)

    while (continue) {
      planInProgress = phases(planInProgress, context)

      if (!planInProgress.query.isSolved) {
        produceAndThrowException(planInProgress)
      }

      planInProgress.query.tail match {
        case None => continue = false
        case Some(q) =>
          val pipe = if (q.containsUpdates && planInProgress.pipe.readsFromDatabase && planInProgress.pipe.isLazy) {
            new EagerPipe(planInProgress.pipe)
          }
          else {
            planInProgress.pipe
          }
          planInProgress = planInProgress.copy(query = q, pipe = pipe)
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
      val descriptor = { () =>
        val result = decorator.decorate(pipe.executionPlanDescription, closingIterator.isEmpty)
        result
      }
      (state, closingIterator, descriptor)
    }
    catch {
      case (t: Throwable) =>
        queryContext.close(success = false)
        throw t
    }
  }

  private def produceAndThrowException(plan: ExecutionPlanInProgress) {
    val errors = builders.flatMap(builder => builder.missingDependencies(plan).map(builder ->)).toList

    if (errors.isEmpty) {
      throw new SyntaxException( """Somehow, Cypher was not able to construct a valid execution plan from your query.
The Neo4j team is very interested in knowing about this query. Please, consider sending a copy of it to cypher@neo4j.org.
Thank you!

The Neo4j Team""")
    }

    val errorMessage = errors.distinct.map(_._2).mkString("\n")
    throw new SyntaxException(errorMessage)
  }

  val phases =
    prepare andThen   /* Prepares the query by rewriting it before other plan builders start working on it. */
    matching andThen  /* Pulls in data from the stores, adds named paths, and filters the result */
    updates andThen   /* Plans update actions */
    extract andThen   /* Handles RETURN and WITH expression */
    finish            /* Prepares the return set so it looks like the user specified */


  lazy val builders = phases.myBuilders.distinct

  /*
  The order of the plan builders here is important. It decides which PlanBuilder gets to go first.
   */
  def prepare = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new PredicateRewriter, 
      new KeyTokenResolver,  
      new AggregationPreparationRewriter(), 
      new IndexLookupBuilder,
      new StartPointChoosingBuilder, 
      new MergeStartPointBuilder,
      new OptionalMatchBuilder(matching)
    )
  }

  def matching = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new TraversalMatcherBuilder,
      new FilterBuilder, 
      new NamedPathBuilder,
      new LoadCSVBuilder,
      new StartPointBuilder,
      new MatchBuilder,
      new UnwindBuilder,
      new ShortestPathBuilder
    )
  }

  def updates = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new NamedPathBuilder,
      new MergePatternBuilder(prepare andThen matching),
      new UpdateActionBuilder
    )
  }

  def extract = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new TopPipeBuilder,
      new ExtractBuilder,
      new SliceBuilder,
      new DistinctBuilder,
      new AggregationBuilder, 
      new SortBuilder
    )
  }

  def finish = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new ColumnFilterBuilder,
      new EmptyResultBuilder
    )
  }
}
