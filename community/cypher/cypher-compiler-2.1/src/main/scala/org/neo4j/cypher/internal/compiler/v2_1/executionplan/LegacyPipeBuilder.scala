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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1.commands._
import org.neo4j.cypher.internal.compiler.v2_1.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.builders._
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.{Monitors, PreparedQuery}
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged

trait ExecutionPlanInProgressRewriter {
  def rewrite(in: ExecutionPlanInProgress)(implicit context: PipeMonitor): ExecutionPlanInProgress
}

class LegacyPipeBuilder(monitors: Monitors, eagernessRewriter: Pipe => Pipe = addEagernessIfNecessary)
  extends PatternGraphBuilder with PipeBuilder with GraphQueryBuilder {

  private implicit val pipeMonitor: PipeMonitor = monitors.newMonitor[PipeMonitor]()

  def producePlan(in: PreparedQuery, planContext: PlanContext): PipeInfo = in.abstractQuery match {
    case q: PeriodicCommitQuery =>
      producePlan(in.copy(abstractQuery = q.query), planContext).
      copy(periodicCommit = Some(PeriodicCommitInfo(q.batchSize)))

    case q: Query =>
      buildQuery(q, planContext)

    case q: IndexOperation =>
      buildIndexQuery(q)

    case q: UniqueConstraintOperation =>
      buildConstraintQuery(q)

    case q: Union =>
      buildUnionQuery(q, planContext)
  }

  private val unionBuilder = new UnionBuilder(this)

  private def buildUnionQuery(union: Union, context: PlanContext)(implicit pipeMonitor: PipeMonitor): PipeInfo =
    unionBuilder.buildUnionQuery(union, context)

  private def buildIndexQuery(op: IndexOperation): PipeInfo = PipeInfo(new IndexOperationPipe(op), updating = true)

  private def buildConstraintQuery(op: UniqueConstraintOperation): PipeInfo = {
    val label = KeyToken.Unresolved(op.label, TokenType.Label)
    val propertyKey = KeyToken.Unresolved(op.propertyKey, TokenType.PropertyKey)

    PipeInfo(new ConstraintOperationPipe(op, label, propertyKey), updating = true)
  }

  def buildQuery(inputQuery: Query, context: PlanContext)(implicit pipeMonitor:PipeMonitor): PipeInfo = {
    val initialPSQ = PartiallySolvedQuery(inputQuery)

    def untilConverged(in: ExecutionPlanInProgress): ExecutionPlanInProgress =
      iterateUntilConverged { input: ExecutionPlanInProgress =>
        val result = phases(input, context)
        if (!result.query.isSolved) {
          produceAndThrowException(result)
        }

        input.query.tail match {
          case None       => result
          case Some(tail) => result.copy(query = tail)
        }
      }(in)

    val planInProgress: ExecutionPlanInProgress =
      untilConverged(ExecutionPlanInProgress(initialPSQ, NullPipe(), isUpdating = false))

    val pipe = eagernessRewriter(planInProgress.pipe)

    PipeInfo(pipe, planInProgress.isUpdating)
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
    prepare andThen /* Prepares the query by rewriting it before other plan builders start working on it. */
    matching andThen /* Pulls in data from the stores, adds named paths, and filters the result */
    updates andThen /* Plans update actions */
    extract andThen /* Handles RETURN and WITH expression */
    finish /* Prepares the return set so it looks like the user specified */


  lazy val builders = phases.myBuilders.distinct

  /*
  The order of the plan builders here is important. It decides which PlanBuilder gets to go first.
   */
  private def prepare = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new UnwindBuilder,
      new LoadCSVBuilder,
      new PredicateRewriter,
      new KeyTokenResolver,
      new IndexLookupBuilder,
      new StartPointChoosingBuilder,
      new MergeStartPointBuilder,
      new OptionalMatchBuilder(matching)
    )
  }

  private def matching = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new TraversalMatcherBuilder,
      new FilterBuilder,
      new NamedPathBuilder,
      new LoadCSVBuilder,
      new StartPointBuilder,
      new MatchBuilder,
      new ShortestPathBuilder,
      new DisconnectedShortestPathEndPointsBuilder
    )
  }

  private def updates = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new NamedPathBuilder,
      new MergePatternBuilder(prepare andThen matching),
      new UpdateActionBuilder
    )
  }

  private def extract = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new TopPipeBuilder,
      new ExtractBuilder,
      new SliceBuilder,
      new DistinctBuilder,
      new AggregationBuilder,
      new SortBuilder
    )
  }

  private def finish = new Phase {
    def myBuilders: Seq[PlanBuilder] = Seq(
      new ColumnFilterBuilder,
      new EmptyResultBuilder
    )
  }
}
