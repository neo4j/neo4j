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
package org.neo4j.fabric.executor

import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryExecutionType.QueryType
import org.neo4j.kernel.api.exceptions.Status

object QueryTypes {

  def merge(a: QueryExecutionType, b: QueryExecutionType): QueryExecutionType = {
    val queryType = merge(a.queryType(), b.queryType())
    (a, b) match {
      case (a, b) if a.isExplained && b.isExplained => QueryExecutionType.explained(queryType)
      case (a, b) if a.isExplained || b.isExplained => invalidCombination(a, b)
      case (a, b) if a.isProfiled && b.isProfiled   => QueryExecutionType.profiled(queryType)
      case (a, b) if a.isProfiled || b.isProfiled   => invalidCombination(a, b)
      case _                                        => QueryExecutionType.query(queryType)
    }
  }

  def merge(a: QueryType, b: QueryType): QueryType = (a, b) match {
    case (a, b) if a == b                                 => a
    case (a, b) if eitherIs(a, b, QueryType.SCHEMA_WRITE) => QueryType.SCHEMA_WRITE
    case (a, b) if eitherIs(a, b, QueryType.DBMS)         => QueryType.DBMS
    case (a, b) if eitherIs(a, b, QueryType.READ_WRITE)   => QueryType.READ_WRITE
    case (a, b) if eitherIs(a, b, QueryType.WRITE)        => QueryType.READ_WRITE
  }

  private def eitherIs(a: QueryType, b: QueryType, expected: QueryType) =
    a == expected || b == expected

  private def invalidCombination(a: QueryExecutionType, b: QueryExecutionType): Nothing =
    throw new FabricException(Status.General.UnknownError, "Invalid combination of query execution types: %s, %s", a, b)
}
