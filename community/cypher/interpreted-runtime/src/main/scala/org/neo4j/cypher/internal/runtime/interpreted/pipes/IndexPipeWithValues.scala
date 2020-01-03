/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.internal.kernel.api.NodeValueIndexCursor

/**
  * Provides a helper method for index pipes that get nodes together with actual property values.
  */
trait IndexPipeWithValues extends Pipe {

  // Name of the node variable
  val ident: String
  // all indices where the index can provide values
  val indexPropertyIndices: Array[Int]
  // the cached node properties where we will get values
  val indexCachedNodeProperties: Array[CachedNodeProperty]

  class IndexIterator(queryContext: QueryContext,
                      baseContext: ExecutionContext,
                      cursor: NodeValueIndexCursor
                     ) extends IndexIteratorBase[ExecutionContext](cursor) {

    override protected def fetchNext(): ExecutionContext = {
      if (cursor.next()) {
        val newContext = executionContextFactory.copyWith(baseContext, ident, queryContext.nodeById(cursor.nodeReference()))
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(indexCachedNodeProperties(i), cursor.propertyValue(indexPropertyIndices(i)))
          i += 1
        }
        newContext
      } else null
    }
  }
}
