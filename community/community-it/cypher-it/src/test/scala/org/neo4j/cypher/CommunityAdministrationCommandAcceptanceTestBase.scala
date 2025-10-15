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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.NotSystemDatabaseException
import org.neo4j.gqlstatus.GqlStatusInfoCodes

abstract class CommunityAdministrationCommandAcceptanceTestBase extends ExecutionEngineFunSuite
    with GraphDatabaseTestSupport {

  val param: String = s"$$param"
  val paramName: String = "param"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  def assertFailure(command: String, errorMsg: String): Unit = {
    the[Exception] thrownBy {
      // WHEN
      execute(command)
      // THEN
    } should have message errorMsg
  }

  def assertFailureWithGQLStatus(
    command: String,
    errorMsg: String,
    gqlStatusCode: GqlStatusInfoCodes,
    statusDescription: String
  ): Unit = {
    val gqlMatcher = gqlStatus(gqlStatusCode, statusDescription)
    val exception = the[Neo4jException] thrownBy {
      // WHEN
      execute(command)
    }
    // THEN
    exception should have message errorMsg
    exception shouldBe gqlMatcher
  }

  def assertFailWhenNotOnSystem(command: String, errorMsgCommand: String): Unit = {
    selectDatabase(DEFAULT_DATABASE_NAME)
    val exception = the[NotSystemDatabaseException] thrownBy {
      // WHEN
      execute(command)
      // THEN
    }
    exception should have message
      s"This is an administration command and it should be executed against the system database: $errorMsgCommand"
    exception.gqlStatusObject().gqlStatus() should be(GqlStatusInfoCodes.STATUS_51N28.getStatusString)
    exception.gqlStatusObject().statusDescription() should be(
      "error: system configuration or operation exception - not supported by this database. This Cypher command must be executed against the database `system`."
    )
  }
}
