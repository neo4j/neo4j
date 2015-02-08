/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan

import builders._
import org.neo4j.cypher.internal.compiler.v1_9.pipes._
import org.neo4j.cypher._
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.cypher.internal.compiler.v1_9.commands.RelationshipByIndexQuery
import org.neo4j.cypher.internal.compiler.v1_9.pipes.EagerPipe
import org.neo4j.cypher.internal.compiler.v1_9.commands.RelationshipByIndex
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{SymbolTable, NodeType, RelationshipType}
import org.neo4j.cypher.internal.compiler.v1_9.mutation.{CreateNode, CreateRelationship}
import org.neo4j.cypher.internal.compiler.v1_9.{ExecutionContext, ClosingIterator}
import org.neo4j.cypher.internal.compiler.v1_9.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v1_9.spi.QueryContext
import org.neo4j.kernel.impl.transaction.LockManager

class ExecutionPlanImpl(inputQuery: Query, graph: GraphDatabaseService) extends ExecutionPlan with PatternGraphBuilder {

  type ExecutionPlanParams = (QueryContext, Transaction, Boolean, Map[String, Any])

  val executionPlan: ExecutionPlanParams => ExecutionResult = prepareExecutionPlan()

  def execute(query: QueryContext, tx: Transaction, params: Map[String, Any]): ExecutionResult = executionPlan(query, tx, false, params)

  def profile(query: QueryContext, tx: Transaction, params: Map[String, Any]): ExecutionResult = executionPlan(query, tx, true, params)

  lazy val lockManager = graph.asInstanceOf[InternalAbstractGraphDatabase].getDependencyResolver.resolveDependency(classOf[LockManager])

  private def prepareExecutionPlan(): ExecutionPlanParams => ExecutionResult = {
    var continue = true
    var planInProgress = ExecutionPlanInProgress(PartiallySolvedQuery(inputQuery), new ParameterPipe(), containsTransaction = false)
    checkFirstQueryPattern(planInProgress)

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

      // This is hard to test. Break up into smaller pieces and test each of them

      planInProgress.query.tail match {
        case None => continue = false
        case Some(q) =>
          val pipe = if (q.containsUpdates && planInProgress.pipe.isLazy) {
            new EagerPipe(planInProgress.pipe)
          }
          else {
            planInProgress.pipe
          }
          planInProgress = planInProgress.copy(query = q, pipe = pipe)
      }
    }

    val columns = getQueryResultColumns(inputQuery, planInProgress.pipe.symbols)

    val pipe = planInProgress.pipe

    if (planInProgress.containsTransaction) {
      getEagerReadWriteQuery(pipe, columns)
    } else {
      getLazyReadonlyQuery(planInProgress.pipe, columns)
    }
  }

  private def checkFirstQueryPattern(planInProgress: ExecutionPlanInProgress) {
    val startPoints = getStartPointsFromPlan(planInProgress.query)
    validatePattern(startPoints, planInProgress.query.patterns.map(_.token))
  }

  private def validatePattern(symbols: SymbolTable, patterns: Seq[Pattern]) = {
    //We build the graph here, because the pattern graph builder finds problems with the pattern
    //that we don't find other wise. This should be moved out from the patternGraphBuilder, but not right now
    buildPatternGraph(symbols, patterns)
  }

  private def getStartPointsFromPlan(query: PartiallySolvedQuery): SymbolTable = {
    val startMap = query.start.map(_.token).map {
      case RelationshipById(varName, _)                                         => varName -> RelationshipType()
      case RelationshipByIndex(varName, _, _, _)                                => varName -> RelationshipType()
      case RelationshipByIndexQuery(varName, _, _)                              => varName -> RelationshipType()
      case AllRelationships(varName: String)                                    => varName -> RelationshipType()
      case CreateRelationshipStartItem(CreateRelationship(varName, _, _, _, _)) => varName -> RelationshipType()

      case NodeByIndex(varName: String, _, _, _)       => varName -> NodeType()
      case NodeByIndexQuery(varName: String, _, _)     => varName -> NodeType()
      case NodeById(varName: String, _)                => varName -> NodeType()
      case AllNodes(varName: String)                   => varName -> NodeType()
      case CreateNodeStartItem(CreateNode(varName, _)) => varName -> RelationshipType()
    }.toMap

    val symbols = SymbolTable(startMap)
    symbols
  }

  private def getQueryResultColumns(q: Query, currentSymbols: SymbolTable) = {
    var query = q
    while (query.tail.isDefined) {
      query = query.tail.get
    }

    val columns = query.returns.columns.flatMap {
      case "*" => currentSymbols.identifiers.keys
      case x   => Seq(x)
    }

    columns
  }

  private def getLazyReadonlyQuery(pipe: Pipe, columns: List[String]): ExecutionPlanParams => ExecutionResult =
    (planParams: ExecutionPlanParams) => {
      val (state, results, planDescriptor) = prepareStateAndResult(planParams, pipe)

      new PipeExecutionResult(results, columns, state, planDescriptor)
  }

  private def getEagerReadWriteQuery(pipe: Pipe, columns: List[String]): ExecutionPlanParams => ExecutionResult = {
    val func = (planParams: ExecutionPlanParams) => {
      val (state, results, planDescriptor) = prepareStateAndResult(planParams, pipe)

      new EagerPipeExecutionResult(results, columns, state, graph, planDescriptor)
    }

    func
  }

  private def prepareStateAndResult(planParams: ExecutionPlanParams, pipe: Pipe):
  (QueryState, ClosingIterator[ExecutionContext], () => PlanDescription) = {
    val (ctx, tx, profile, params) = planParams

    try {

      val decorator: PipeDecorator = if (profile) new Profiler() else NullDecorator
      val state = new QueryState(ctx, params, decorator)
      val results = pipe.createResults(state)
      val closingIterator = new ClosingIterator[ExecutionContext](results, state.query, tx)
      val descriptor = () => decorator.decorate(pipe.executionPlanDescription, closingIterator.isEmpty)

      (state, closingIterator, descriptor)
    } catch {
      case e: Throwable =>
        tx.failure()
        tx.close()
        throw e
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
    new NodeByIdBuilder(graph),
    new IndexQueryBuilder(graph),
    new GraphGlobalStartBuilder(graph),
    new FilterBuilder,
    new NamedPathBuilder,
    new ExtractBuilder,
    new MatchBuilder,
    new SortBuilder,
    new ColumnFilterBuilder,
    new SliceBuilder,
    new AggregationBuilder,
    new ShortestPathBuilder,
    new RelationshipByIdBuilder(graph),
    new CreateNodesAndRelationshipsBuilder(graph),
    new UpdateActionBuilder(graph),
    new EmptyResultBuilder,
    new TraversalMatcherBuilder(graph),
    new TopPipeBuilder,
    new DistinctBuilder
  )
}
