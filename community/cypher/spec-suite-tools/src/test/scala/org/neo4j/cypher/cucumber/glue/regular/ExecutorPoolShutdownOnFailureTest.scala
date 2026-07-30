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
package org.neo4j.cypher.cucumber.glue.regular

import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.cucumber.glue.regular.TestConf.Settings
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService

/**
 * A `startDbms`-adjacent setup failure must not leave an already-started `DatabaseManagementService` orphaned.
 * An orphaned DBMS keeps its native buffers (transaction log, checkpoint, id-generator segments) registered
 * forever with the process-wide `BufferLeakTracker`, which eventually fails an unrelated test's session-close check.
 */
class ExecutorPoolShutdownOnFailureTest extends CypherFunSuite {

  test("createExecutor shuts down the just-started DBMS when accessorFrom fails") {
    val dbms = mock[DatabaseManagementService]
    when(dbms.database(SYSTEM_DATABASE_NAME)).thenThrow(new RuntimeException("system database is unavailable"))

    val pool = new FixedStartDbmsPool(dbms)

    val thrown = intercept[RuntimeException] {
      pool.createExecutorForTest(Map.empty)
    }

    thrown.getMessage should include("system database is unavailable")
    verify(dbms).shutdown()
  }
}

/** Overrides `startDbms` to hand out a pre-built (mocked) dbms, so `createExecutor`'s own fix is what's under test. */
private class FixedStartDbmsPool(dbms: DatabaseManagementService) extends ExecutorPool {
  override def conf: TestConf = TestConf(useEnterprise = false)
  override protected def startDbms(extraSettings: Settings): DatabaseManagementService = dbms

  def createExecutorForTest(extraSettings: Settings): DbAccessor = createExecutor(extraSettings)
}
