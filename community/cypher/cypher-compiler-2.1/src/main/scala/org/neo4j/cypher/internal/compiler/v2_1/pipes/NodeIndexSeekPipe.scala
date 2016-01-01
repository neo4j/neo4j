/**
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.commands.{QueryExpression, indexQuery}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.{Index, IntroducedIdentifier}
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_1.symbols.{CTNode, SymbolTable}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.index.IndexDescriptor

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             propertyKey: PropertyKeyToken,
                             valueExpr: QueryExpression[Expression],
                             unique: Boolean = false)
                            (implicit pipeMonitor: PipeMonitor) extends Pipe {

  val descriptor = new IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  val indexFactory: (QueryState) => (Any) => Iterator[Node] =
    if (unique)
      (state: QueryState) => (x: Any) => state.query.exactUniqueIndexSearch(descriptor, x).toIterator
    else
      (state: QueryState) => (x: Any) => state.query.exactIndexSearch(descriptor, x)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val index = indexFactory(state)
    val resultNodes = indexQuery(valueExpr, ExecutionContext.empty, state, index, label.name, propertyKey.name)
    resultNodes.map(node => ExecutionContext.from(ident -> node))
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescription = {
    val name = if (unique) "NodeUniqueIndexSeek" else "NodeIndexSeek"
    new PlanDescriptionImpl(this, name, NoChildren, Seq(
      IntroducedIdentifier(ident), Index(label.name, propertyKey.name))
    )
  }

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects.READS_NODES
}
