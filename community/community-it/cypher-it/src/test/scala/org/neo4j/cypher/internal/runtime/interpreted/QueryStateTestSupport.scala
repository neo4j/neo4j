/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommunityCypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type

trait QueryStateTestSupport {
  self: GraphDatabaseTestSupport =>

  def withQueryState[T](txType: KernelTransaction.Type = Type.EXPLICIT)(f: QueryState => T) = {
    val tx = graph.beginTransaction(txType, AUTH_DISABLED)
    try {
      QueryStateHelper.withQueryState(
        graph,
        tx,
        Array.empty,
        queryState => {
          queryState.setExecutionContextFactory(CommunityCypherRowFactory())
          f(queryState)
        }
      )
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    } finally {
      tx.close()
    }
  }

  def withCountsQueryState[T](f: QueryState => T) = {
    val tx = graph.beginTransaction(Type.EXPLICIT, AUTH_DISABLED)
    try {
      QueryStateHelper.withQueryState(
        graph,
        tx,
        Array.empty,
        queryState => {
          val state = QueryStateHelper.countStats(queryState)
          state.setExecutionContextFactory(CommunityCypherRowFactory())
          f(state)
        }
      )
    } finally {
      tx.close()
    }
  }

}
