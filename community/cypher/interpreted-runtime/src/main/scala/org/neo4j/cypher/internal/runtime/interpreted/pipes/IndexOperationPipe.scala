/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.opencypher.v9_0.util.SyntaxException
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.{CreateIndex, DropIndex, IndexOperation}
import org.opencypher.v9_0.util.attribution.Id

case class IndexOperationPipe(indexOp: IndexOperation)(val id: Id = Id.INVALID_ID) extends Pipe {
  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val queryContext = state.query

    val labelId = queryContext.getOrCreateLabelId(indexOp.label)

    indexOp match {
      case CreateIndex(_, propertyKeys, _) =>
        val propertyKeyIds: Seq[Int] = propertyKeys.map( queryContext.getOrCreatePropertyKeyId )
        queryContext.addIndexRule(IndexDescriptor(labelId, propertyKeyIds))

      case DropIndex(_, propertyKeys, _) =>
        val propertyKeyIds: Seq[Int] = propertyKeys.map( queryContext.getOrCreatePropertyKeyId )
        queryContext.dropIndexRule(IndexDescriptor(labelId, propertyKeyIds))

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
}
