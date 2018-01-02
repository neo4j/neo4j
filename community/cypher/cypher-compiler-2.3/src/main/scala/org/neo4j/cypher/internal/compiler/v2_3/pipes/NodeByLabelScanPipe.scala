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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsNodesWithLabels}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.LabelName
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class NodeByLabelScanPipe(ident: String, label: LazyLabel)
                              (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {

    label.id(state.query) match {
      case Some(labelId) =>
        val nodes = state.query.getNodesByLabel(labelId.id)
        val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
        nodes.map(n => baseContext.newWith1(ident, n))
      case None =>
        Iterator.empty
    }
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = new PlanDescriptionImpl(this.id, "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), identifiers)

  def symbols = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodesWithLabels(label.name))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
