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
package org.neo4j.internal.collector

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.collector.DataCollectorMatchers.beMapContaining

class DataCollectorTestSupport extends ExecutionEngineFunSuite {

  val IDLE = "idle"
  val COLLECTING = "collecting"

  protected def assertInvalidArgument(query: String): Unit = {
    val e = intercept[CypherExecutionException](execute(query))
    e.status should be(org.neo4j.kernel.api.exceptions.Status.General.InvalidArguments)
  }

  protected def assertIdle(section: String): Unit = assertStatus(IDLE, section)

  protected def assertCollecting(section: String): Unit = assertStatus(COLLECTING, section)

  protected def assertStatus(status: String, section: String): Unit = {
    val res = execute("CALL db.stats.status()").single
    res should beMapContaining(
      "status" -> status,
      "section" -> section
    )
  }
}
