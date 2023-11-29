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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ConfigurationBuilder

import scala.jdk.CollectionConverters.CollectionHasAsScala

class InternalNotificationTest extends CypherFunSuite {

  private val reflections = new Reflections(new ConfigurationBuilder()
    .forPackages("org.neo4j")
    .addScanners(Scanners.SubTypes))

  private def subTypes[T](cls: Class[T]): Iterable[Class[_ <: T]] = {
    reflections.getSubTypesOf(cls).asScala
  }

  /**
   * If you add or remove an internal notification, the set InternalNotification.allNotifications should be updated
   * accordingly. This set is used for creating corresponding metrics that track the count of issued notifications.
   */
  test("All internal notifications should be listed in InternalNotification.allNotifications") {
    InternalNotification.allNotifications should equal(
      subTypes(classOf[InternalNotification]).map(_.getSimpleName.stripSuffix("$")).toSet
    )
  }
}
