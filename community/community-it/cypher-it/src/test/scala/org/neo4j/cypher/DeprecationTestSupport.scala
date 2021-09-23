/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.impl.notification.NotificationCode
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.scalatest.Matchers
import org.scalatest.Suite

trait DeprecationTestSupport extends Suite with Matchers {

  protected val dbms: FeatureDatabaseManagementService

  private val lastMajorCypherVersion = List("CYPHER 3.5")
  private val supportedCypherVersions_4_X = List("CYPHER 4.3", "CYPHER 4.4")
  private val supportedCypherVersions = lastMajorCypherVersion ++ supportedCypherVersions_4_X

  def assertNotificationInSupportedVersions(query: String,
                                            notificationCode: NotificationCode,
                                            details: NotificationDetail*
                                           ): Unit = {
    assertNotificationInSupportedVersions(Seq(query), notificationCode, details: _*)
  }

  def assertNotificationInSupportedVersions(
                                             queries: Seq[String],
                                             notificationCode: NotificationCode,
                                             details: NotificationDetail*
                                           ): Unit = {
    assertNotification(supportedCypherVersions, queries, shouldContainNotification = true, notificationCode, details: _*)
  }

  def assertNoNotificationInSupportedVersions(query: String,
                                              notificationCode: NotificationCode,
                                              details: NotificationDetail*
                                             ): Unit = {
    assertNoNotificationInSupportedVersions(Seq(query), notificationCode, details: _*)
  }

  def assertNoNotificationInSupportedVersions(
                                               queries: Seq[String],
                                               notificationCode: NotificationCode,
                                               details: NotificationDetail*
                                             ): Unit = {
    assertNotification(supportedCypherVersions, queries, shouldContainNotification = false, notificationCode, details: _*)
  }

  def assertNotificationInSupportedVersions_4_X(
                                                 query: String,
                                                 notificationCode: NotificationCode,
                                                 details: NotificationDetail*
                                               ): Unit = {
    assertNotificationInSupportedVersions_4_X(Seq(query), notificationCode, details: _*)
  }

  def assertNotificationInSupportedVersions_4_X(
                                                 queries: Seq[String],
                                                 notificationCode: NotificationCode,
                                                 details: NotificationDetail*
                                               ): Unit = {
    assertNotification(supportedCypherVersions_4_X, queries, shouldContainNotification = true, notificationCode, details: _*)
  }

  def assertNoNotificationInSupportedVersions_4_X(
                                                   query: String,
                                                   notificationCode: NotificationCode,
                                                   details: NotificationDetail*
                                                 ): Unit = {
    assertNotification(supportedCypherVersions_4_X, Seq(query), shouldContainNotification = false, notificationCode, details: _*)
  }

  def assertNotificationInLastMajorVersion(
                                            query: String,
                                            notificationCode: NotificationCode,
                                            details: NotificationDetail*
                                          ): Unit = {
    assertNotificationInLastMajorVersion(Seq(query), notificationCode, details: _*)
  }

  def assertNotificationInLastMajorVersion(
                                            queries: Seq[String],
                                            notificationCode: NotificationCode,
                                            details: NotificationDetail*
                                          ): Unit = {
    assertNotification(lastMajorCypherVersion, queries, shouldContainNotification = true, notificationCode, details: _*)
  }

  def assertNoNotificationInLastMajorVersion(
                                              query: String,
                                              notificationCode: NotificationCode,
                                              details: NotificationDetail*
                                            ): Unit = {
    assertNoNotificationInLastMajorVersion(Seq(query), notificationCode, details: _*)
  }

  def assertNoNotificationInLastMajorVersion(
                                              queries: Seq[String],
                                              notificationCode: NotificationCode,
                                              details: NotificationDetail*
                                            ): Unit = {
    assertNotification(lastMajorCypherVersion, queries, shouldContainNotification = false, notificationCode, details: _*)
  }

  private def assertNotification(
                                  versions: List[String],
                                  queries: Seq[String],
                                  shouldContainNotification: Boolean,
                                  notificationCode: NotificationCode,
                                  details: NotificationDetail*): Unit = {
    queries.foreach(query => {
      versions.foreach(version => {
        withClue(s"Failed for query '$query' in version $version \n") {
          val transaction = dbms.begin()
          try {
            val result = transaction.execute(s"$version $query")
            val notifications: Iterable[Notification] = result.getNotifications()
            val hasNotification = notifications.exists(notification => matchesCode(notification, notificationCode, details: _*))
            hasNotification should be(shouldContainNotification)
          } finally {
            transaction.rollback()
          }
        }
      })
    })
  }

  private def matchesCode(notification: Notification, notificationCode: NotificationCode, details: NotificationDetail*): Boolean = {
    // In this test class we are not interested in the exact input position
    val expected = notificationCode.notification(InputPosition.empty, details: _*)
    notification.getCode.equals(expected.getCode) &&
      notification.getDescription.equals(expected.getDescription) &&
      notification.getSeverity.equals(expected.getSeverity)
  }
}
