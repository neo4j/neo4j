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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.{QueryExpression, indexQuery}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsLabel, ReadsNodeProperty, ReadsNodes}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.Index
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.{CTNode, SymbolTable}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.index.IndexDescriptor

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             propertyKey: PropertyKeyToken,
                             valueExpr: QueryExpression[Expression],
                             unique: Boolean = false)
                            (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe with RonjaPipe {

  private val descriptor = new IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  private val indexFactory: (QueryState) => (Any) => Iterator[Node] =
    if (unique)
      (state: QueryState) => (x: Any) => state.query.exactUniqueIndexSearch(descriptor, x).toIterator
    else
      (state: QueryState) => (x: Any) => state.query.exactIndexSearch(descriptor, x)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    val index = indexFactory(state)
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val resultNodes = indexQuery(valueExpr, baseContext, state, index, label.name, propertyKey.name)
    resultNodes.map(node => baseContext.newWith1(ident, node))
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = {
    val name = if (unique) "NodeUniqueIndexSeek" else "NodeIndexSeek"
    new PlanDescriptionImpl(this.id, name, NoChildren, Seq(Index(label.name, propertyKey.name)), identifiers)
  }

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodes, ReadsLabel(label.name), ReadsNodeProperty(propertyKey.name))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
