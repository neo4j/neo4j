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
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException

case class IndexOperationPipe(indexOp: IndexOperation)(implicit val monitor: PipeMonitor) extends Pipe {
  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val queryContext = state.query

    val labelId = queryContext.getOrCreateLabelId(indexOp.label)

    indexOp match {
      case CreateIndex(_, propertyKeys, _) =>
        val propertyKeyIds: Seq[Int] = propertyKeys.map( queryContext.getOrCreatePropertyKeyId )
        queryContext.addIndexRule(labelId, single(propertyKeyIds))

      case DropIndex(_, propertyKeys, _) =>
        val propertyKeyIds: Seq[Int] = propertyKeys.map( queryContext.getOrCreatePropertyKeyId )
        queryContext.dropIndexRule(labelId, single(propertyKeyIds))

      case _ =>
        throw new UnsupportedOperationException("Unknown IndexOperation encountered")
    }

    Iterator.empty
  }

  private def single[T](s: Seq[T]): T = {
    if (s.isEmpty || s.tail.nonEmpty)
      throw new SyntaxException("Cypher support only one property key per index right now")
    s(0)
  }

  def symbols = new SymbolTable()

  def planDescription = PlanDescriptionImpl(this.id, indexOp.toString, NoChildren, Seq.empty, identifiers)

  def exists(pred: Pipe => Boolean) = pred(this)

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects()
}
