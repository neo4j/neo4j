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
import internal.spi.QueryContext
import internal.{ExecutionContext, ClosingIterator}
import internal.commands._
import internal.mutation.{CreateNode, CreateRelationship}
import internal.symbols.{NodeType, RelationshipType, SymbolTable}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.ExecutionResult

class ExecutionPlanBuilder(graph: GraphDatabaseService) extends PatternGraphBuilder {

  def build(inputQuery: AbstractQuery): ExecutionPlan = {

    val (p, isUpdating) = buildPipes(inputQuery)

    val columns = getQueryResultColumns(inputQuery, p.symbols)
    val (pipe, func) = if (isUpdating) {
      (p, getEagerReadWriteQuery(p, columns))
    } else {
      (p, getLazyReadonlyQuery(p, columns))
    }

    val executionPlanDescription = pipe.executionPlanDescription

    new ExecutionPlan {
      def execute(queryContext: QueryContext, params: Map[String, Any]) = func(queryContext, params)
      def description = executionPlanDescription.toString
    }
  }

  def buildPipes(in: AbstractQuery): (Pipe, Boolean) = in match {
    case q: Query          => buildQuery(q)
    case q: IndexOperation => buildIndexQuery(q)
    case q: Union          => buildUnionQuery(q)
  }

  val unionBuilder = new UnionBuilder(this)

  def buildUnionQuery(union: Union): (Pipe, Boolean) = unionBuilder.buildUnionQuery(union)

  def buildIndexQuery(op: IndexOperation): (Pipe, Boolean) = (new IndexOperationPipe(op), true)

  def buildQuery(inputQuery: Query): (Pipe, Boolean) = {
    var continue = true
    var planInProgress = ExecutionPlanInProgress(PartiallySolvedQuery(inputQuery), new ParameterPipe(), isUpdating = false)
    checkFirstQueryPattern(planInProgress)

    while (continue) {
      while (builders.exists(_.canWorkWith(planInProgress))) {
        val matchingBuilders = builders.filter(_.canWorkWith(planInProgress))

        val builder = matchingBuilders.sortBy(_.priority).head
        val newPlan = builder(planInProgress)

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
          validatePattern(planInProgress.pipe.symbols, planInProgress.query.patterns.map(_.token))
      }
    }

    // TODO Get back to this when we touch the type system
    (new ResultValueMapperPipe(planInProgress.pipe), planInProgress.isUpdating)
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

      case NodeByIndex(varName: String, _, _, _)             => varName -> NodeType()
      case NodeByIndexQuery(varName: String, _, _)           => varName -> NodeType()
      case NodeById(varName: String, _)                      => varName -> NodeType()
      case AllNodes(varName: String)                         => varName -> NodeType()
      case CreateNodeStartItem(CreateNode(varName, _, _, _)) => varName -> RelationshipType()
    }.toMap

    val symbols = new SymbolTable(startMap)
    symbols
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

    case _ => List.empty
  }

  private def prepareStateAndResult(queryContext: QueryContext, params: Map[String, Any], pipe: Pipe): (QueryState, Iterator[ExecutionContext]) = {
    val state = new QueryState(graph, queryContext, params)
    val results = pipe.createResults(state)

    try {
      val closingIterator = new ClosingIterator[ExecutionContext](results, queryContext)
      (state, closingIterator)
    }
    catch {
      case (t: Throwable) =>
        queryContext.close(success = false)
        throw t
    }
  }

  private def getLazyReadonlyQuery(pipe: Pipe, columns: List[String]): ((QueryContext, Map[String, Any]) => ExecutionResult) = {
    val func = (queryContext: QueryContext, params: Map[String, Any]) => {
      val (state, results) = prepareStateAndResult(queryContext, params, pipe)

      new PipeExecutionResult(results, columns, state, pipe.executionPlanDescription.toString)
    }
    func
  }

  private def getEagerReadWriteQuery(pipe: Pipe, columns: List[String]): ((QueryContext, Map[String, Any]) => ExecutionResult) = {
    val func = (queryContext: QueryContext, params: Map[String, Any]) => {
      val (state, results) = prepareStateAndResult(queryContext, params, pipe)
      new EagerPipeExecutionResult(results, columns, state, graph, pipe.executionPlanDescription.toString)
    }
    func
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