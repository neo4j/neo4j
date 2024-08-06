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

import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

trait DeprecationTestSupport extends Suite with Matchers {

  protected val dbms: FeatureDatabaseManagementService

  def assertNotification(
    queries: Seq[String],
    shouldContainNotification: Boolean,
    details: String,
    createNotification: (InputPosition, String) => Notification,
    cypherVersions: Set[CypherVersion] = CypherVersion.values
  ): Unit = {
    cypherVersions.foreach { version =>
      queries.foreach(query => {
        withClue(s"Failed in Cypher version ${version.version} for query '$query' \n") {
          val transaction = dbms.begin()
          try {
            val result = transaction.execute(s"EXPLAIN CYPHER ${version.version} $query")
            val notifications: Iterable[Notification] = result.getNotifications()
            val hasNotification =
              notifications.exists(notification =>
                matchesCode(notification, details, createNotification)
              )
            withClue(notifications) {
              hasNotification should be(shouldContainNotification)
            }
          } finally {
            transaction.rollback()
          }
        }
      })
    }
  }

  def assertNotification(
    queries: Seq[String],
    shouldContainNotification: Boolean,
    createNotification: InputPosition => Notification
  ): Unit = {
    assertNotification(
      queries,
      shouldContainNotification,
      "",
      (pos, _) => createNotification(pos),
      CypherVersion.values
    )
  }

  def assertNotification(
    queries: Seq[String],
    shouldContainNotification: Boolean,
    createNotification: InputPosition => Notification,
    cypherVersions: Set[CypherVersion]
  ): Unit = {
    assertNotification(
      queries,
      shouldContainNotification,
      "",
      (pos, _) => createNotification(pos),
      cypherVersions
    )
  }

  // this is hacky but we have no other way to probe the notification's status (`Status.Statement.FeatureDeprecationWarning`)
  private def isDeprecation(notification: Notification): Boolean =
    notification.getTitle == "This feature is deprecated and will be removed in future versions."

  def assertNoDeprecations(
    queries: Seq[String],
    cypherVersions: Set[CypherVersion] = CypherVersion.values
  ): Unit = {
    cypherVersions.foreach { version =>
      queries.foreach(query =>
        withClue(s"Failed in Cypher version ${version.version} for query '$query' \n") {
          val transaction = dbms.begin()
          try {
            val result = transaction.execute(s"EXPLAIN CYPHER ${version.version} $query")
            val deprecations = result.getNotifications().filter(isDeprecation)
            withClue(
              s"""Expected no notifications to be found but was:
                 |${deprecations.map(_.getDescription).mkString("'", "', '", "'")}
                 |""".stripMargin
            ) {
              deprecations shouldBe empty
            }
          } finally {
            transaction.rollback()
          }
        }
      )
    }
  }

  private def matchesCode(
    notification: Notification,
    details: String,
    createNotification: (InputPosition, String) => Notification
  ): Boolean = {
    // In this test class we are not interested in the exact input position
    val expected = createNotification(InputPosition.empty, details)
    notification.getCode.equals(expected.getCode) &&
    notification.getDescription.equals(expected.getDescription) &&
    notification.getSeverity.equals(expected.getSeverity)
    // TODO: also compare messages once those are public (https://trello.com/c/VoNT60cD/100-make-notification-visible-to-the-drivers)
  }
}
