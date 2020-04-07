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
package org.neo4j.cypher

import java.lang.Boolean.TRUE

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED
import org.neo4j.graphdb.impl.notification.NotificationDetail

class RuntimeUnsupportedNotificationTest extends ExecutionEngineFunSuite {

  test("Should say when an enterprise runtime is not supported on community") {
    val result = execute("CYPHER runtime=pipelined EXPLAIN RETURN 1")
    result.notifications should contain(RUNTIME_UNSUPPORTED.notification(InputPosition.empty,
      NotificationDetail.Factory.message("Runtime unsupported", "This version of Neo4j does not support requested runtime: pipelined")))
  }

  test("can also be configured to fail hard") {
    restartWithConfig(Map(GraphDatabaseSettings.cypher_hints_error -> TRUE))
    eengine = createEngine(graph)

    val exception = intercept[RuntimeUnsupportedException](execute("CYPHER runtime=pipelined EXPLAIN RETURN 1"))
    exception.getMessage should be ("This version of Neo4j does not support requested runtime: pipelined")
  }
}
