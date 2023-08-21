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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.kernel.impl.locking.LockClientStoppedException
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class SystemCommandExecutionPlanTest extends CypherFunSuite {

  private val mockSystemUpdateCountingQueryContext = mock[SystemUpdateCountingQueryContext]

  test("will call QueryHandler to translate exceptions") {
    // Given
    val queryHandler = QueryHandler.handleError((e, p) => {
      new CypherExecutionException("Message " + p.get("k1").asInstanceOf[StringValue].stringValue())
    })
    val subscriber =
      new SystemCommandQuerySubscriber(
        mockSystemUpdateCountingQueryContext,
        new QuerySubscriberAdapter {},
        queryHandler,
        VirtualValues.map(Array("k1"), Array(Values.stringValue("v1")))
      )

    // WHEN
    subscriber.onError(new LockClientStoppedException(null))

    // THEN
    the[CypherExecutionException] thrownBy subscriber.assertNotFailed() should have message ("Message v1")

  }

}
