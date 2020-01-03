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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime._
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryExecutionType.QueryType

object QueryTypeConversion {

  def asPublic(internal: InternalQueryType): QueryType =
    internal match {
      case READ_ONLY => QueryExecutionType.QueryType.READ_ONLY
      case READ_WRITE => QueryExecutionType.QueryType.READ_WRITE
      case WRITE => QueryExecutionType.QueryType.WRITE
      case SCHEMA_WRITE => QueryExecutionType.QueryType.SCHEMA_WRITE
      case DBMS => QueryExecutionType.QueryType.DBMS
    }
}
