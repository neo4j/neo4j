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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.virtual.ListValue

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class Command(
  private val defaultColumns: List[CommandDefaultColumn],
  private val yieldColumns: List[CommandYieldColumn]
) {
  private val columns: List[CommandYieldColumn] = getColumns(defaultColumns, yieldColumns)

  // The requested columns for the command,
  // only these will be returned to the user and need to be generated
  protected val requestedColumnsNames: List[String] = {
    // we want the original column names here as we want to match on the names for the columns we want to create
    val yieldedColumns = yieldColumns.map(_.originalName)
    // Make sure to get the yielded columns if YIELD was specified
    // otherwise get the default columns
    if (yieldedColumns.isEmpty) defaultColumns.map(_.name)
    else yieldedColumns
  }

  protected def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]]

  final def rows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    originalNameRows(state, baseRow).map { map =>
      columns.map {
        // We want the original column names for the map as the original rows uses them
        case CommandYieldColumn(originalName, aliasedName) => aliasedName -> map(originalName)
      }.toMap
    }
  }

  protected def getConfiguredTimeZone(ctx: QueryContext): ZoneId =
    ctx.getConfig.get(GraphDatabaseSettings.db_timezone).getZoneId

  protected def formatTime(startTime: Long, zoneId: ZoneId): OffsetDateTime =
    OffsetDateTime.ofInstant(Instant.ofEpochMilli(startTime), zoneId)

  // Make sure to get the yielded columns (and their potential renames) if YIELD was specified
  // otherwise get the default columns
  private def getColumns(
    defaultColumns: List[CommandDefaultColumn],
    yieldColumns: List[CommandYieldColumn]
  ): List[CommandYieldColumn] = {
    if (yieldColumns.nonEmpty) yieldColumns
    else defaultColumns.map(c => CommandYieldColumn(c.name, c.name))
  }
}

object Command {

  // Get the string values from `names`, removing possible duplicates (keeps NO_VALUE/null from Cypher 25)
  // names could for example be the id lists for `SHOW TRANSACTIONS ['id1', 'id2']`
  protected[showcommands] def extractNames(
    names: Option[Expression],
    state: QueryState,
    baseRow: CypherRow,
    originOperation: String,
    cypherVersion: CypherVersion
  ): List[String] =
    names.map(e =>
      e(baseRow, state) match {
        case s: StringValue => List(s.stringValue())
        case l: ListValue =>
          val list = l.iterator().asScala
          list.map {
            case s: StringValue                                                     => s.stringValue()
            case _: NoValue if cypherVersion.isEqualOrAfter(CypherVersion.Cypher25) => null
            case x =>
              throw ParameterWrongTypeException.expectedStringButGotValue(
                originOperation,
                String.valueOf(x),
                x.prettify()
              )
          }.toSet.toList
        case _: NoValue if cypherVersion.isEqualOrAfter(CypherVersion.Cypher25) => List(null)
        case x =>
          throw ParameterWrongTypeException.expectedStringOrStringList(
            originOperation,
            String.valueOf(x),
            x.prettify()
          )
      }
    ).getOrElse(List.empty)
}
