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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{RelationshipType, NodeType}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PlanBuilder, ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{MutableMaps, Pipe, NullPipe, FakePipe}
import org.junit.Assert._
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.commands.Query

trait BuilderTest extends Assertions {
  def createPipe(nodes: Seq[String] = Seq(), relationships: Seq[String] = Seq()): FakePipe = {
    val nodeIdentifiers = nodes.map(x => x -> NodeType())
    val relIdentifiers = relationships.map(x => x -> RelationshipType())

    new FakePipe(Seq(MutableMaps.empty), (nodeIdentifiers ++ relIdentifiers): _*)
  }

  // for avoiding missing an override while refactoring
  final def plan(q: PartiallySolvedQuery): ExecutionPlanInProgress = plan(NullPipe, q)

  final def plan(p: Pipe, q: PartiallySolvedQuery): ExecutionPlanInProgress = ExecutionPlanInProgress(q, p)

  def assertAccepts(q: PartiallySolvedQuery): ExecutionPlanInProgress = assertAccepts(plan(q))

  def assertAccepts(q: Query): ExecutionPlanInProgress = assertAccepts(PartiallySolvedQuery(q))

  def assertAccepts(p: Pipe, q: PartiallySolvedQuery): ExecutionPlanInProgress = assertAccepts(plan(p, q))

  def assertAccepts(planInProgress: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    assertTrue("Should be able to build on this", builder.canWorkWith(planInProgress, context))
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
    assertFalse("Should not accept this", builder.canWorkWith(planInProgress, context))
  }

  def builder: PlanBuilder
  def context:PlanContext=null
}