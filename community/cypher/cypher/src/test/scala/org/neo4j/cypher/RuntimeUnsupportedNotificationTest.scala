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
package org.neo4j.cypher

import org.neo4j.cypher.ExecutionEngineHelper._
import org.neo4j.cypher.internal.frontend.v3_3.notification.RuntimeUnsupportedNotification
import org.neo4j.graphdb.factory.GraphDatabaseSettings

import scala.language.reflectiveCalls

class RuntimeUnsupportedNotificationTest extends ExecutionEngineFunSuite {

  test("default behaviour is to not fail when asked for compiled runtime") {
    val result = execute("cypher runtime=compiled explain return 42")
    result.notifications should contain(RuntimeUnsupportedNotification)
  }

  test("can also be configured to fail hard") {
    graph.shutdown()
    graph = createGraphDatabase(Map(GraphDatabaseSettings.cypher_hints_error -> "true"))
    eengine = createEngine(graph)

    intercept[InvalidArgumentException](execute("cypher runtime=compiled return 42"))
  }
}
