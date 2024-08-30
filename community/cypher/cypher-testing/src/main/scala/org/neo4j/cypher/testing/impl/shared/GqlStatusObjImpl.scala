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

import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.SeverityLevel

import java.util

case class GqlStatusObjImpl(
  status: String,
  statusDescr: String,
  diagnosticRec: util.Map[String, AnyRef],
  severity: SeverityLevel,
  position: InputPosition,
  classification: NotificationClassification
) extends GqlStatusObject {
  override def gqlStatus(): String = status
  override def statusDescription(): String = statusDescr
  override def diagnosticRecord(): util.Map[String, AnyRef] = diagnosticRec
  override def getSeverity: SeverityLevel = severity
  override def getPosition: InputPosition = position
  override def getClassification: NotificationClassification = classification
}

object GqlStatusObjImpl {

  def fromRaw(
    status: String,
    statusDescr: String,
    diagnosticRec: util.Map[String, AnyRef],
    severity: String,
    position: InputPosition,
    classification: String
  ): GqlStatusObjImpl =
    GqlStatusObjImpl(
      status,
      statusDescr,
      diagnosticRec,
      SeverityLevel.valueOf(severity),
      position,
      NotificationClassification.valueOf(classification)
    )
}
