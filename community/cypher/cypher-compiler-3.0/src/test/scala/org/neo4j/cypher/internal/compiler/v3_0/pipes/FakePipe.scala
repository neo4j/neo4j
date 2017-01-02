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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, SingleRowPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CypherType
import org.scalatest.mock.MockitoSugar

import scala.collection.Map

class FakePipe(val data: Iterator[Map[String, Any]], newVariables: (String, CypherType)*) extends Pipe with MockitoSugar {

  def this(data: Traversable[Map[String, Any]], variables: (String, CypherType)*) = this(data.toIterator, variables:_*)

  val symbols = SymbolTable(newVariables.toMap)

  def internalCreateResults(state: QueryState) = data.map(m => ExecutionContext(collection.mutable.Map(m.toSeq: _*)))

  def planDescription: InternalPlanDescription = SingleRowPlanDescription(this.id, variables = variables)

  def exists(pred: Pipe => Boolean) = ???

  val monitor: PipeMonitor = mock[PipeMonitor]

  def dup(sources: List[Pipe]): Pipe = ???

  override def localEffects = Effects()

  override def sources: Seq[Pipe] = Seq.empty
}
