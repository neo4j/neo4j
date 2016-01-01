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

import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.{ExecutionContext, symbols}
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.graphdb.Relationship


case class DirectedRelationshipByIdSeekPipe(ident: String, relIdExpr: Seq[Expression], toNode: String, fromNode: String)
                                           (implicit pipeMonitor: PipeMonitor) extends Pipe with CollectionSupport {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val relIds = relIdExpr.flatMap(expr => Option(expr.apply(ExecutionContext.empty)(state)))
    new IdSeekIterator[Relationship](ident, state.query.relationshipOps, relIds.iterator).map {
      ctx =>
        val r = ctx(ident)
        r match {
          case r: Relationship => ctx += (fromNode -> r.getStartNode) += (toNode -> r.getEndNode)
        }
    }
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescription = new PlanDescriptionImpl(
    pipe = this,
    name = "DirectedRelationshipByIdSeekPipe",
    children = NoChildren,
    arguments = Seq(
      Arguments.IntroducedIdentifier(ident),
      Arguments.IntroducedIdentifier(toNode),
      Arguments.IntroducedIdentifier(fromNode)) ++ relIdExpr.map(e => Arguments.LegacyExpression(e))
  )

  def symbols = new SymbolTable(Map(ident -> CTRelationship, toNode -> CTNode, fromNode -> CTNode))

  def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects.READS_ENTITIES
}
