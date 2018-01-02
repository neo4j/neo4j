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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.InternalException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class UnionPipe(sources: List[Pipe], columns:List[String])(implicit val monitor: PipeMonitor) extends Pipe {
  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = new UnionIterator(sources, state)

  def planDescription: InternalPlanDescription =
    sources.map(_.planDescription).reduce[InternalPlanDescription] {
      case (l, r) => new PlanDescriptionImpl(this.id, "Union", TwoChildren(l, r), Seq.empty, identifiers)
    }

  def symbols = new SymbolTable(columns.map(k => k -> CTAny).toMap)

  def exists(pred: Pipe => Boolean) = pred(this) || sources.exists(_.exists(pred))

  def dup(sources: List[Pipe]): Pipe = {
    if (sources.length != this.sources.length)
      throw new InternalException("Cannot changes the number of pipes when rewriting")

    copy(sources = sources)
  }

  override def localEffects = Effects()
}

case class NewUnionPipe(l: Pipe, r: Pipe)
                       (val estimatedCardinality: Option[Double] = None)(implicit val monitor: PipeMonitor)
  extends Pipe with RonjaPipe {
  def planDescriptionWithoutCardinality: InternalPlanDescription =
    new PlanDescriptionImpl(this.id, "Union", TwoChildren(l.planDescription, r.planDescription), Seq.empty, identifiers)

  def symbols: SymbolTable = l.symbols intersect r.symbols

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    l.createResults(state) ++ r.createResults(state)

  def exists(pred: Pipe => Boolean): Boolean = l.exists(pred) || r.exists(pred)

  def dup(sources: List[Pipe]): Pipe = {
    val (l :: r :: Nil) = sources
    copy(l, r)(estimatedCardinality)
  }

  def sources: Seq[Pipe] = Seq(l, r)

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
