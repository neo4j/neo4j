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
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherException
import org.neo4j.kernel.impl.query.TransactionalContext
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer

/**
  * Cypher compiler, which compiles pre-parsed queries into executable queries.
  */
trait Compiler {

  /**
    * Compile pre-parsed query into executable query.
    *
    * @param preParsedQuery pre-parsed query to convert
    * @param tracer compilation tracer to which events of the compilation process are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @param transactionalContext transactional context to use during compilation (in logical and physical planning)
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a compiled and executable query
    */
  @throws[org.neo4j.cypher.CypherException]
  def compile(preParsedQuery: PreParsedQuery,
              tracer: CompilationPhaseTracer,
              preParsingNotifications: Set[org.neo4j.graphdb.Notification],
              transactionalContext: TransactionalContext
             ): CacheableExecutableQuery
}
