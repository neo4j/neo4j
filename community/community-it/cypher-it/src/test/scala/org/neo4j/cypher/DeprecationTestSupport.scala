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
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.kernel.api.exceptions.NotificationCategory
import org.neo4j.notifications.StandardGqlStatusObject
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

trait DeprecationTestSupport extends Suite with Matchers {

  protected val dbms: FeatureDatabaseManagementService

  def assertNotification(
    queries: Seq[String],
    shouldContainNotification: Boolean,
    details: String,
    createNotification: (InputPosition, String) => Notification,
    expectedGqlStatusObjects: List[TestGqlStatusObject],
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

            val gqlStatusObjects: List[GqlStatusObject] = result.getGqlStatusObjects().toList
            gqlStatusObjects.size should be(expectedGqlStatusObjects.size)

            val actualGqlStatusObject = gqlStatusObjects.map(gso =>
              TestGqlStatusObject(gso.gqlStatus(), gso.statusDescription(), gso.getSeverity, gso.getClassification)
            )

            actualGqlStatusObject should contain theSameElementsAs expectedGqlStatusObjects
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
    createNotification: InputPosition => Notification,
    expectedGqlStatusObjects: List[TestGqlStatusObject]
  ): Unit = {
    assertNotification(
      queries,
      shouldContainNotification,
      "",
      (pos, _) => createNotification(pos),
      expectedGqlStatusObjects,
      CypherVersion.values
    )
  }

  def assertNotification(
    queries: Seq[String],
    shouldContainNotification: Boolean,
    createNotification: InputPosition => Notification,
    expectedGqlStatusObjects: List[TestGqlStatusObject],
    cypherVersions: Set[CypherVersion]
  ): Unit = {
    assertNotification(
      queries,
      shouldContainNotification,
      "",
      (pos, _) => createNotification(pos),
      expectedGqlStatusObjects,
      cypherVersions
    )
  }

  private def isDeprecation(notification: Notification): Boolean =
    notification.getCategory == NotificationCategory.DEPRECATION

  private def isDeprecation(gso: GqlStatusObject): Boolean =
    gso.getClassification == NotificationClassification.DEPRECATION

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
            val deprecationsNotificationApi = result.getNotifications().filter(isDeprecation)
            withClue(
              s"""Expected no deprecations to be found using the Notification API but was:
                 |${deprecationsNotificationApi.map(_.getDescription).mkString("'", "', '", "'")}
                 |""".stripMargin
            ) {
              deprecationsNotificationApi shouldBe empty
            }

            val deprecationsGqlStatusAPI = result.getGqlStatusObjects().filter(isDeprecation)
            withClue(
              s"""Expected no deprecations to be found using the GqlStatusObject API but was:
                 |${deprecationsGqlStatusAPI.map(_.gqlStatus()).mkString("'", "', '", "'")}
                 |""".stripMargin
            ) {
              deprecationsGqlStatusAPI shouldBe empty
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
  }

  case class TestGqlStatusObject(
    gqlStatus: String,
    statusDescription: String,
    severity: SeverityLevel,
    classification: NotificationClassification
  )

  val testOmittedResult: TestGqlStatusObject = TestGqlStatusObject(
    StandardGqlStatusObject.OMITTED_RESULT.gqlStatus(),
    StandardGqlStatusObject.OMITTED_RESULT.statusDescription(),
    SeverityLevel.UNKNOWN,
    NotificationClassification.UNKNOWN
  )
}
