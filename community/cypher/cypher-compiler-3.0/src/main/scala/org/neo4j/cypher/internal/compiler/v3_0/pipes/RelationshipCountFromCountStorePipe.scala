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
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.CountRelationshipsExpression
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.NameId

case class RelationshipCountFromCountStorePipe(ident: String, startLabel: Option[LazyLabel],
                                                 typeNames: LazyTypes, endLabel: Option[LazyLabel],
                                                 bothDirections: Boolean)
                                                (val estimatedCardinality: Option[Double] = None)
                                                (implicit pipeMonitor: PipeMonitor) extends Pipe with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val labelIds: Seq[Int] = Seq(startLabel, endLabel).map {
      case Some(label) =>
        val labelId: Int = label.id(state.query) match {
          case Some(x) => x
          case _ => throw new IllegalArgumentException("Cannot find id for label: " + label)
        }
        labelId
      case _ => NameId.WILDCARD
    }
    val count = if (bothDirections)
      countOneDirection(state, typeNames, labelIds) + countOneDirection(state, typeNames, labelIds.reverse)
    else
      countOneDirection(state, typeNames, labelIds)
    Seq(baseContext.newWith1(ident, count)).iterator
  }

  def countOneDirection(state: QueryState, typeNames: LazyTypes, labelIds: Seq[Int]) =
    typeNames.types(state.query) match {
      case None => state.query.relationshipCountByCountStore(labelIds.head, NameId.WILDCARD, labelIds(1))
      case Some(types) => types.foldLeft(0L) { (count, typeId) =>
        count + state.query.relationshipCountByCountStore(labelIds.head, typeId, labelIds(1))
      }
    }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = PlanDescriptionImpl(
    this.id, "CountStoreRelationshipAggregation", NoChildren,
    Seq(CountRelationshipsExpression(ident, startLabel, typeNames, endLabel, bothDirections)), identifiers)

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
