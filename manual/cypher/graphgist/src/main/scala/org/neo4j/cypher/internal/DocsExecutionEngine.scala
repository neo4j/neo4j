/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal

import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ExecutionEngine, SyntaxException}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}
import org.neo4j.logging.{LogProvider, NullLogProvider}

import scala.collection.JavaConverters._

class DocsExecutionEngine(graph: GraphDatabaseService, logProvider: LogProvider = NullLogProvider.getInstance)
                         (implicit monitor: QueryExecutionMonitor, session: QuerySession)
  extends ExecutionEngine(graph, logProvider) {

  @throws(classOf[SyntaxException])
  def internalExecute(query: String, params: JavaMap[String, Any]): InternalExecutionResult =
    RewindableExecutionResult(execute(query, params.asScala.toMap))

  @throws(classOf[SyntaxException])
  def internalProfile(query: String, params: JavaMap[String, Any]): InternalExecutionResult =
    RewindableExecutionResult(profile(query, params.asScala.toMap))
}
