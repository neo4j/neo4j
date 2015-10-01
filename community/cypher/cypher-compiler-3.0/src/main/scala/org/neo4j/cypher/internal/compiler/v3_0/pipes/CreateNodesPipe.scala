/*
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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{CreatesAnyNode, Effects, ReadsAllNodes}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.{UpdateAction, CreateNode, GraphElementPropertyFunctions}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.CreateNodes
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.{ParameterWrongTypeException, InternalException}
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphdb.NotInTransactionException

case class CreateNodesPipe(actions: Seq[CreateNode])(val estimatedCardinality: Option[Double] = None)
                           (implicit pipeMonitor: PipeMonitor) extends Pipe with RonjaPipe  {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    executeMutationCommands(baseContext, state)
  }

  private def executeMutationCommands(ctx: ExecutionContext,
                                      state: QueryState): Iterator[ExecutionContext] =
    try {
      actions.foldLeft(Iterator(ctx))((context, cmd) => context.flatMap(c => cmd.exec(c, state)))
    } catch {
      case e: NotInTransactionException =>
        throw new InternalException("Expected to be in a transaction at this point", e)
    }

  override def updating = true

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = PlanDescriptionImpl(this.id, "CreateNodes", NoChildren, Seq(), identifiers)

  def symbols = new SymbolTable(actions.map(_.key -> CTNode).toMap)

  override def monitor = pipeMonitor

  override def localEffects: Effects = Effects(CreatesAnyNode)

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
