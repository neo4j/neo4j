/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class ConstraintOperationPipe(op: UniqueConstraintOperation, label: KeyToken, propertyKey: KeyToken)
                             (implicit val monitor: PipeMonitor) extends Pipe {
  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val labelId = label.getOrCreateId(state.query)
    val propertyKeyId = propertyKey.getOrCreateId(state.query)

    op match {
      case _: CreateUniqueConstraint => state.query.createUniqueConstraint(labelId, propertyKeyId)
      case _: DropUniqueConstraint   => state.query.dropUniqueConstraint(labelId, propertyKeyId)
    }

    Iterator.empty
  }

  def symbols = new SymbolTable()

  def planDescription = new PlanDescriptionImpl(this, "ConstraintOperation", NoChildren, Seq.empty, identifiers)

  def exists(pred: Pipe => Boolean) = pred(this)

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override val localEffects = Effects()
}
