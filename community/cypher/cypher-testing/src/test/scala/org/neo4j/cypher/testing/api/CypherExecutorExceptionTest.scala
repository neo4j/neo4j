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
package org.neo4j.cypher.testing.api

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.exceptions.Status

class CypherExecutorExceptionTest extends CypherFunSuite {

  test("should be able to extract status from executor exception") {
    val originalStatus = Status.Statement.ExecutionFailed
    val exception = CypherExecutorException(originalStatus, new Exception, Some("Test"))
    val extractedStatus = Status.statusCodeOf(exception)
    extractedStatus should equal(originalStatus)
  }
}
