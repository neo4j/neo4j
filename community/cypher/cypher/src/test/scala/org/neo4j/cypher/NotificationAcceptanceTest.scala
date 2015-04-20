/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.InputPosition
import org.neo4j.cypher.internal.compiler.v2_3.notification.CartesianProductNotification


class NotificationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("Warn for cartesian product") {
    val result = executeWithAllPlanners("explain match (a)-->(b), (c)-->(d) return *")

    result.notifications.toList should equal(List(CartesianProductNotification(InputPosition(7, 1, 8))))
  }

  test("Don't warn for cartesian product when not using explain") {
    val result = executeWithAllPlanners("match (a)-->(b), (c)-->(d) return *")

    result.notifications shouldBe empty
  }
}
