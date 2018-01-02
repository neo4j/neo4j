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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsGivenNodeProperty, ReadsNodesWithLabels}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.Index
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNode

case class NodeIndexScanPipe(ident: String,
                             label: LabelToken,
                             propertyKey: PropertyKeyToken)
                            (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe with RonjaPipe {

  private val descriptor = IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val resultNodes = state.query.indexScan(descriptor)
    resultNodes.map(node => baseContext.newWith1(ident, node))
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality =
    new PlanDescriptionImpl(this.id, "NodeIndexScan", NoChildren, Seq(Index(label.name, propertyKey.name)), identifiers)

  def symbols = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodesWithLabels(label.name), ReadsGivenNodeProperty(propertyKey.name))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
