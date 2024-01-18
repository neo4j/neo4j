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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

/**
 * Runtime test utility methods that may have implementations that dependent on the edition (community or enterprise).
 */
trait RuntimeTestUtils {

  /**
   * Extracts the query statistics from the given query state, independent of runtime implementation.
   */
  def queryStatistics(queryState: AnyRef): org.neo4j.cypher.internal.runtime.QueryStatistics
}

object CommunityRuntimeTestUtils extends RuntimeTestUtils {

  override def queryStatistics(state: AnyRef): org.neo4j.cypher.internal.runtime.QueryStatistics = {
    state.asInstanceOf[QueryState].getStatistics
  }
}
