/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.CountNodesExpression
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.NameId
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

case class NodeCountFromCountStorePipe(ident: String, label: Option[LazyLabel])(val estimatedCardinality: Option[Double] = None)
                                                           (implicit pipeMonitor: PipeMonitor) extends Pipe with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val count = label match {
      case Some(lazyLabel) => lazyLabel.getOptId(state.query) match {
        case Some(idOfLabel) => state.query.nodeCountByCountStore(idOfLabel)
        case _ => 0
      }
      case _ => state.query.nodeCountByCountStore(NameId.WILDCARD)
    }
    Seq(baseContext.newWith1(ident, count)).iterator
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = PlanDescriptionImpl(
    this.id, "NodeCountFromCountStore", NoChildren, Seq(CountNodesExpression(ident, label)), variables)

  def symbols = new SymbolTable(Map(ident -> CTInteger))

  override def monitor = pipeMonitor

  override def localEffects: Effects = Effects()

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
