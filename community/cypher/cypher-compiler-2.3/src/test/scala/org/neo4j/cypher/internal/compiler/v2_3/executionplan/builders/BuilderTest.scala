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

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.UpdateAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

trait BuilderTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  def createPipe(nodes: Seq[String] = Seq(), relationships: Seq[String] = Seq()): FakePipe = {
    val nodeIdentifiers = nodes.map(x => x -> CTNode)
    val relIdentifiers = relationships.map(x => x -> CTRelationship)

    new FakePipe(Seq(MutableMaps.empty), (nodeIdentifiers ++ relIdentifiers): _*)
  }

  // for avoiding missing an override while refactoring
  final def plan(q: PartiallySolvedQuery): ExecutionPlanInProgress = plan(SingleRowPipe(), q)

  final def plan(q: Query): ExecutionPlanInProgress = plan(SingleRowPipe(), PartiallySolvedQuery(q))

  final def plan(p: Pipe, q: PartiallySolvedQuery): ExecutionPlanInProgress = ExecutionPlanInProgress(q, p)

  def assertAccepts(q: PartiallySolvedQuery): ExecutionPlanInProgress = assertAccepts(plan(q))

  def assertAccepts(q: Query): ExecutionPlanInProgress = assertAccepts(PartiallySolvedQuery(q))

  def assertAccepts(p: Pipe, q: PartiallySolvedQuery): ExecutionPlanInProgress = assertAccepts(plan(p, q))

  def assertAccepts(planInProgress: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    withClue("Should be able to build on this."){
      builder.canWorkWith(planInProgress, context) should equal(true)
    }
    builder.apply(planInProgress, context)
  }

  def assertRejects(q: PartiallySolvedQuery) {
    assertRejects(plan(q))
  }

  def assertRejects(q: Query) {
    assertRejects(PartiallySolvedQuery(q))
  }

  def assertRejects(p: Pipe, q: PartiallySolvedQuery) {
    assertRejects(plan(p, q))
  }

  def assertRejects(planInProgress: ExecutionPlanInProgress) {
    withClue("Should not accept this")(builder.canWorkWith(planInProgress, context)) should equal(false)
  }

  def newQuery(start: Seq[StartItem] = Seq(),
               where: Seq[Predicate] = Seq(),
               updates: Seq[UpdateAction] = Seq(),
               patterns: Seq[Pattern] = Seq(),
               returns: Seq[ReturnColumn] = Seq(),
               tail: Option[PartiallySolvedQuery] = None) =
    PartiallySolvedQuery().copy(
      start = start.map(Unsolved(_)),
      where = where.map(Unsolved(_)),
      patterns = patterns.map(Unsolved(_)),
      returns = returns.map(Unsolved(_)),
      updates = updates.map(Unsolved(_)),
      tail = tail
    )

  def builder: PlanBuilder
  var context:PlanContext=null
}
