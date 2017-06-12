/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.phases

import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification

/**
 * A NotificationLogger records notifications.
 */
sealed trait InternalNotificationLogger {
  def log(notification: InternalNotification)

  def notifications: Set[InternalNotification]
}

/**
  * A null implementation that discards all notifications.
  */
case object devNullLogger extends InternalNotificationLogger {
  override def log(notification: InternalNotification) {}

  override def notifications: Set[InternalNotification] = Set.empty
}

/**
  * NotificationLogger that records all notifications for later retrieval.
  */
class RecordingNotificationLogger extends InternalNotificationLogger {
  private val builder = Set.newBuilder[InternalNotification]

  def log(notification: InternalNotification) = builder += notification

  def notifications = builder.result()
}
