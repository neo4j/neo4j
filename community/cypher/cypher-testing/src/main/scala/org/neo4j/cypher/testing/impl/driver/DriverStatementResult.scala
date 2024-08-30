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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.shared.GqlStatusObjImpl
import org.neo4j.cypher.testing.impl.shared.NotificationImpl
import org.neo4j.driver
import org.neo4j.driver.NotificationSeverity
import org.neo4j.driver.Result
import org.neo4j.driver.internal.value.StringValue
import org.neo4j.driver.summary
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.NotificationCategory
import org.neo4j.graphdb.SeverityLevel

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

case class DriverStatementResult(private val driverResult: Result) extends StatementResult {

  override def columns(): Seq[String] = driverResult.keys().asScala.toList

  override def records(): Seq[Record] = driverResult.list().asScala.toList.map(toValue)

  private def toValue(record: driver.Record): Map[String, AnyRef] =
    record.asMap[AnyRef](DriverRecordConverter.convertValue).asScala.toMap

  override def consume(): Unit = driverResult.consume()

  override def getNotifications(): List[Notification] =
    driverResult.consume().notifications().asScala.toList
      .map(n =>
        NotificationImpl.fromRaw(
          n.code,
          n.title,
          n.description,
          n.rawSeverityLevel().orElseGet(() => NotificationSeverity.INFORMATION.toString),
          Option(n.position())
            .map(pos => new InputPosition(pos.offset(), pos.line(), pos.column()))
            .getOrElse(InputPosition.empty),
          n.rawCategory().orElseGet(() => NotificationCategory.UNKNOWN.name())
        )
      )

  override def getGqlStatusObjects(): List[GqlStatusObject] =
    driverResult.consume().gqlStatusObjects().asScala.toList
      .map((gso: summary.GqlStatusObject) => {
        val diagnosticRecord = gso.diagnosticRecord().asInstanceOf[java.util.Map[String, Object]]

        val rawPosition = diagnosticRecord.get("_position")
        val position = if (rawPosition != null) {
          val positionMap = rawPosition.asInstanceOf[org.neo4j.driver.internal.value.MapValue]
          new InputPosition(
            positionMap.get("offset").asInstanceOf[org.neo4j.driver.internal.value.IntegerValue].asInt(),
            positionMap.get("line").asInstanceOf[org.neo4j.driver.internal.value.IntegerValue].asInt(),
            positionMap.get("column").asInstanceOf[org.neo4j.driver.internal.value.IntegerValue].asInt()
          )
        } else {
          InputPosition.empty
        }

        GqlStatusObjImpl.fromRaw(
          gso.gqlStatus(),
          gso.statusDescription(),
          diagnosticRecord,
          diagnosticRecord
            .getOrDefault("_severity", new StringValue(SeverityLevel.UNKNOWN.name()))
            .asInstanceOf[StringValue]
            .asString(),
          position,
          diagnosticRecord
            .getOrDefault("_classification", new StringValue(NotificationClassification.UNKNOWN.name()))
            .asInstanceOf[StringValue]
            .asString()
        )
      })

  override def iterator(): Iterator[Map[String, AnyRef]] = driverResult.asScala.map(toValue)
  override def close(): Unit = {}
}
