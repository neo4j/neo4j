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
package org.neo4j.cypher.internal.javacompat

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QueryExecution

class ResultSubscriberTest extends CypherFunSuite {

  test("onError should suppress errors") {
    val subscriber = new ResultSubscriber(null, null)

    val exception1 = new RuntimeException("e1")
    val exception2 = new RuntimeException("e2")
    subscriber.onError(exception1)
    subscriber.onError(exception2)

    val thrown = the[Exception] thrownBy subscriber.init(mock[QueryExecution])
    thrown should be theSameInstanceAs exception1
    thrown.getSuppressed.toSeq should equal(Seq(exception2))
  }

  test("assertNoErrors (e.g. init) should suppress errors thrown from close") {
    val subscriber = new ResultSubscriber(null, null)

    val exception1 = new RuntimeException("e1")
    val exception2 = new RuntimeException("e2")
    subscriber.onError(exception1)

    val execution = mock[QueryExecution]
    when(execution.cancel()).thenThrow(exception2)

    val thrown = the[Exception] thrownBy subscriber.init(execution)
    thrown should be theSameInstanceAs exception1
    thrown.getSuppressed.toSeq should equal(Seq(exception2))
  }
}
