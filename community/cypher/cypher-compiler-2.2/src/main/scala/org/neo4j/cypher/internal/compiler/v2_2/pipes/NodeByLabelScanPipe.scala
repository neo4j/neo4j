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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, _}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{IntroducedIdentifier, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{SymbolTable, _}

case class NodeByLabelScanPipe(ident: String, label: Either[String, LabelId])
                              (val estimatedCardinality: Option[Long] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val optLabelId = label match {
      case Left(str)      => state.query.getOptLabelId(str).map(LabelId)
      case Right(labelId) => Some(labelId)
    }

    optLabelId match {
      case Some(labelId) =>
        val nodes = state.query.getNodesByLabel(labelId.id)
        val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
        nodes.map(n => baseContext.newWith1(ident, n))
      case None =>
        Iterator.empty
    }
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  private def labelName = label match {
    case Left(name) => name
    case Right(id) => id.id.toString
  }

  def planDescription = new PlanDescriptionImpl(this, "NodeByLabelScan", NoChildren, Seq(IntroducedIdentifier(ident), LabelName(labelName)))

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects.READS_NODES

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}
