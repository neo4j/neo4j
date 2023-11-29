/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util

import java.util.concurrent.atomic.LongAdder

import scala.collection.concurrent.TrieMap

/**
 * A NotificationLogger records notifications.
 */
sealed trait InternalNotificationLogger {
  def log(notification: InternalNotification): Unit

  def notifications: Set[InternalNotification]
}

/**
 * A null implementation that discards all notifications.
 */
case object devNullLogger extends InternalNotificationLogger {
  override def log(notification: InternalNotification): Unit = {}

  override def notifications: Set[InternalNotification] = Set.empty
}

/**
 * NotificationLogger that records all notifications for later retrieval.
 */
class RecordingNotificationLogger() extends InternalNotificationLogger {
  private val builder = Set.newBuilder[InternalNotification]

  def log(notification: InternalNotification): Unit = builder += notification

  def notifications: Set[InternalNotification] = builder.result()
}

case class InternalNotificationStats() {
  private val notificationCounts: TrieMap[String, LongAdder] = new TrieMap()

  def incrementNotificationCount(notification: InternalNotification): Unit = {
    val counts = notificationCounts.getOrElseUpdate(notification.notificationName, new LongAdder)
    counts.increment()
  }

  def getNotificationCount(notificationName: String): Long = {
    notificationCounts.get(notificationName) match {
      case Some(l) => l.longValue()
      case _       => 0L
    }
  }
}
