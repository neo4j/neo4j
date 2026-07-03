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

import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.InternalNotifications
import org.neo4j.cypher.internal.util.test_helpers.DiffPrinter
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ConfigurationBuilder

import scala.jdk.CollectionConverters.CollectionHasAsScala

class InternalNotificationTest extends CypherPlannerTestSuite {

  private val reflections = new Reflections(new ConfigurationBuilder()
    .forPackages("org.neo4j")
    .addScanners(Scanners.SubTypes))

  private def subTypes[T](cls: Class[T]): Iterable[Class[_ <: T]] = {
    reflections.getSubTypesOf(cls).asScala
  }

  /**
   * If you add or remove an internal notification, the set InternalNotifications.allNotifications should be updated
   * accordingly. This set is used for creating corresponding metrics that track the count of issued notifications.
   */
  test("All internal notifications should be listed in InternalNotifications.allNotifications") {
    val actualSet = InternalNotifications.allNotifications
    val expectedSet = subTypes(classOf[InternalNotification]).map(_.getSimpleName.stripSuffix("$")).toSet
    if (actualSet != expectedSet) {
      val actualSeq = actualSet.toSeq.sorted
      val expectedSeq = expectedSet.toSeq.sorted
      val maxWidth = Math.max(actualSeq.map(_.length).max, expectedSeq.map(_.length).max) + 10
      val maxHeightActual = actualSet.size + 10
      val maxHeightExpected = expectedSet.size + 10
      fail(
        s"""Not all internal notifications are listed in InternalNotifications.allNotifications
           |
           |Diff condensed (expected -> actual):
           |------------------------------------
           |${DiffPrinter.render(
            pprint.apply(expectedSeq, width = maxWidth, height = maxHeightExpected).render,
            pprint.apply(actualSeq, width = maxWidth, height = maxHeightActual).render,
            isCondensed = true
          )}
           |
           |Diff full (expected -> actual):
           |-------------------------------
           |${DiffPrinter.render(
            pprint.apply(expectedSeq, width = maxWidth, height = maxHeightExpected).render,
            pprint.apply(actualSeq, width = maxWidth, height = maxHeightActual).render,
            isCondensed = false
          )}
           |
           |---
           |Actual:
           |
           |${pprint.apply(actualSeq, width = maxWidth, height = maxHeightActual)}
           |---
           |Expected:
           |
           |${pprint.apply(expectedSeq, width = maxWidth, height = maxHeightExpected)}
           |---
           |""".stripMargin
      )
    }
  }
}
