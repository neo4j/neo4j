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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.graphdb.Relationship

case class UndirectedRelationshipByIdSeekPipe(ident: String, relIdExpr: Seq[Expression], toNode: String, fromNode: String)
                                             (implicit pipeMonitor: PipeMonitor) extends Pipe with CollectionSupport {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val relIds = relIdExpr.flatMap(expr => Option(expr.apply(ExecutionContext.empty)(state)))
    new IdSeekIterator[Relationship](ident, state.query.relationshipOps, relIds.iterator).flatMap {
      ctx =>
        val r = ctx(ident) match {
          case r: Relationship => r
          case x => throw new InternalException(s"Expected a relationship, got $x")
        }

        val s = r.getStartNode
        val e = r.getEndNode

        Seq(
          ctx.newWith(Seq(ident -> r, toNode -> e, fromNode -> s)),
          ctx.newWith(Seq(ident -> r, toNode -> s, fromNode -> e))
        )
    }
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescription = new PlanDescriptionImpl(this, "UndirectedRelationshipByIdSeek", NoChildren, Seq(
    IntroducedIdentifier(ident),
    IntroducedIdentifier(toNode),
    IntroducedIdentifier(fromNode)
  ))

  def symbols = new SymbolTable(Map(ident -> CTRelationship, toNode -> CTNode, fromNode -> CTNode))

  def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    assert(sources.isEmpty)
    this
  }

  override def localEffects = relIdExpr.effects

  def sources: Seq[Pipe] = Seq.empty
}
