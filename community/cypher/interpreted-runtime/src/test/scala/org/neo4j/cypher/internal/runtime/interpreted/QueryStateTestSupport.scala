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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

trait QueryStateTestSupport {
  self: GraphDatabaseTestSupport =>

  def withQueryState[T](f: QueryState => T) = {
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AUTH_DISABLED)
    try {
      QueryStateHelper.withQueryState(graph, tx, EMPTY_MAP, queryState => {
        f(queryState)
      })
    } finally {
      tx.close()
    }
  }

  def withCountsQueryState[T](f: QueryState => T) = {
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AUTH_DISABLED)
    try {
      QueryStateHelper.withQueryState(graph, tx, EMPTY_MAP, queryState =>
        {
          val state = QueryStateHelper.countStats(queryState)
          f(state)
        })
    } finally {
      tx.close()
    }
  }

}
