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
package org.neo4j.cypher.util

import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.notifications.StandardGqlStatusObject.isStandardGqlStatusCode
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IterableHasAsScala

trait WarningAssertions {
  self: Assertions with Matchers =>

  def shouldHaveWarning(result: Result, statusCode: Status, detailMessage: String, gqlStatus: String): Unit = {
    val notifications: Iterable[Notification] = result.getNotifications.asScala
    val gqlStatusObjects: Iterable[GqlStatusObject] = result.getGqlStatusObjects.asScala

    withClue(
      s"Expected a notification with status code: $statusCode and detail message: $detailMessage\nBut got: $notifications"
    ) {
      notifications.exists { notification =>
        notification.getCode == statusCode.code().serialize() &&
        notification.getDescription == detailMessage
      } should be(true)
    }

    withClue(
      s"Expected a GQL-status objects with GQLSTATUS: $gqlStatus\nBut got: $gqlStatusObjects"
    ) {
      gqlStatusObjects.exists { gqlStatusObject =>
        gqlStatusObject.gqlStatus.equals(gqlStatus)
      } should be(true)
    }
  }

  def shouldHaveNoWarnings(result: Result): Unit = {
    result.getNotifications.asScala should be(empty)
    result.getGqlStatusObjects.asScala.filter(status => !isStandardGqlStatusCode(status)) should be(empty)
  }
}
