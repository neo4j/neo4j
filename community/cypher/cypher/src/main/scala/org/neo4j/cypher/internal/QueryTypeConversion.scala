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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime.DBMS
import org.neo4j.cypher.internal.runtime.DBMS_READ
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.READ_ONLY
import org.neo4j.cypher.internal.runtime.READ_WRITE
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.runtime.WRITE
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryExecutionType.QueryType

object QueryTypeConversion {

  def asPublic(internal: InternalQueryType): QueryType =
    internal match {
      case READ_ONLY    => QueryExecutionType.QueryType.READ_ONLY
      case READ_WRITE   => QueryExecutionType.QueryType.READ_WRITE
      case WRITE        => QueryExecutionType.QueryType.WRITE
      case SCHEMA_WRITE => QueryExecutionType.QueryType.SCHEMA_WRITE
      case DBMS         => QueryExecutionType.QueryType.DBMS
      case DBMS_READ    => QueryExecutionType.QueryType.DBMS
    }
}
