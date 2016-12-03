/*
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable

case class UnionPipe(l: Pipe, r: Pipe)
                    (val id: Id = new Id)
                    (implicit val monitor: PipeMonitor)
  extends Pipe {
  def symbols: SymbolTable = l.symbols intersect r.symbols

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    l.createResults(state) ++ r.createResults(state)

  def exists(pred: Pipe => Boolean): Boolean = l.exists(pred) || r.exists(pred)

  def dup(sources: List[Pipe]): Pipe = {
    val (l :: r :: Nil) = sources
    copy(l, r)(id)
  }

  def sources: Seq[Pipe] = Seq(l, r)
}
