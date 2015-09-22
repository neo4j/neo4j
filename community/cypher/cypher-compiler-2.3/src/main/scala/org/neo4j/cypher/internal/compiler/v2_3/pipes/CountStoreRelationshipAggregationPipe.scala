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

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.NameId
import org.neo4j.cypher.internal.frontend.v2_3.ast.RelTypeName
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class CountStoreRelationshipAggregationPipe(ident: String, startLabel: Option[LazyLabel],
                                                 typeNames: Seq[RelTypeName], endLabel: Option[LazyLabel])
                                                (val estimatedCardinality: Option[Double] = None)
                                                (implicit pipeMonitor: PipeMonitor) extends Pipe with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val labelIds: Seq[Int] = Seq(startLabel, endLabel).map { labelOption =>
      labelOption match {
        case Some(label) =>
          val labelId: Int = label.id(state.query) match {
            case Some(id) => id
            case _ => throw new IllegalArgumentException("Cannot find id for label: " + label)
          }
          labelId
        case _ => NameId.WILDCARD
      }
    }
    val count = if (typeNames.isEmpty) {
      state.query.relationshipCountByCountStore(labelIds(0), NameId.WILDCARD, labelIds(1))
    } else {
      typeNames.foldLeft(0L) { (count, typeName) =>
        val typeId = state.query.getRelTypeId(typeName.name)
        count + state.query.relationshipCountByCountStore(labelIds(0), typeId, labelIds(1))
      }
    }
    Seq(baseContext.newWith1(s"count($ident)", count)).iterator
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = PlanDescriptionImpl(this.id, "CountStoreRelationshipAggregation", NoChildren, Seq(), identifiers)

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
