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
package org.neo4j.cypher

import org.neo4j.graphdb.SeverityLevel

class LegacyPlannerNotificationAcceptanceTest extends ExecutionEngineFunSuite {

  test("should warn when using PLANNER X") {
    val notifications = eengine.execute("EXPLAIN PLANNER RULE MATCH (a) RETURN a").notifications

    notifications should have size 1

    val notification = notifications.head
    notification.getCode should equal("Neo.ClientNotification.Statement.DeprecationWarning")
    notification.getDescription should equal("Using PLANNER for switching between planners has been deprecated, please use CYPHER planner=[rule,cost] instead")
    notification.getSeverity should equal(SeverityLevel.WARNING)
    notification.getTitle should equal("This feature is deprecated and will be removed in future versions.")
  }

  test("should not warn when using planner=x") {
    val notifications = eengine.execute("EXPLAIN CYPHER planner=rule MATCH (a) RETURN a").notifications

    notifications shouldBe empty
  }
}
