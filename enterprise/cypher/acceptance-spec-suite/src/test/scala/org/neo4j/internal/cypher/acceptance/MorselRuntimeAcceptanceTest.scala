/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class MorselRuntimeAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not use morsel by default") {
    //Given
    val result = graph.execute("MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should be able to ask for morsel") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should fallback if morsel doesn't support query") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-[*]->(m) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should warn that morsels are experimental") {
    //Given
    import scala.collection.JavaConverters._

    val result = graph.execute("CYPHER runtime=morsel EXPLAIN MATCH (n) RETURN n")

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (use the morsel runtime at " +
                                                     "your own peril, not recommended to be run on production systems)")

  }
}
