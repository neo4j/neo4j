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
package org.neo4j.cypher.testing.impl.shared

import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.NotificationCategory
import org.neo4j.graphdb.SeverityLevel

case class NotificationImpl(
  code: String,
  title: String,
  description: String,
  severity: SeverityLevel,
  position: InputPosition,
  category: NotificationCategory
) extends Notification {
  override def getCode: String = code
  override def getTitle: String = title
  override def getDescription: String = description
  override def getSeverity: SeverityLevel = severity
  override def getPosition: InputPosition = position
  override def getCategory: NotificationCategory = category
}

object NotificationImpl {

  def fromRaw(
    code: String,
    title: String,
    description: String,
    severity: String,
    position: InputPosition,
    category: String
  ): NotificationImpl =
    NotificationImpl(
      code,
      title,
      description,
      SeverityLevel.valueOf(severity),
      position,
      NotificationCategory.valueOf(category)
    )

  def fromRaw(
    code: String,
    title: String,
    description: String,
    severity: String,
    posOffset: Int,
    posLine: Int,
    posColumn: Int,
    category: String
  ): NotificationImpl =
    fromRaw(code, title, description, severity, new InputPosition(posOffset, posLine, posColumn), category)

}
