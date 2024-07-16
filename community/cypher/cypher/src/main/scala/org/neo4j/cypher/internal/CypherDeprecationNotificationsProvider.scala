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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.graphdb.Notification
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.DeprecationNotificationsProvider

import java.util.function.BiConsumer

abstract class CypherDeprecationNotificationsProvider(queryOptionsOffset: InputPosition)
  extends DeprecationNotificationsProvider {

  protected def notifications: Set[InternalNotification]

  override def forEachDeprecation(consumer: BiConsumer[String, Notification]): Unit = {
    notifications.foreach { n =>
      val notification = NotificationWrapping.asKernelNotification(Some(queryOptionsOffset))(n)
      if (notification.getStatus == Status.Statement.FeatureDeprecationWarning ||
        notification.getStatus == Status.Statement.MissingAlias) {
        consumer.accept(n.notificationName, notification)
      }
    }
  }
}

object CypherDeprecationNotificationsProvider {

  def fromIterables(
    queryOptionsOffset: InputPosition,
    notificationIterables: Iterable[InternalNotification]*
  ): CypherDeprecationNotificationsProvider = {
    new CypherDeprecationNotificationsProvider(queryOptionsOffset) {
      override protected def notifications: Set[InternalNotification] =
        notificationIterables.flatten.toSet
    }
  }
}
