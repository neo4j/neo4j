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
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

import java.util.Locale

import scala.jdk.CollectionConverters.IteratorHasAsScala

case object ServerOptionsConverter extends OptionsConverter[ServerOptions] {
  private val ALLOWED_DATABASES = "allowedDatabases"
  private val DENIED_DATABASES = "deniedDatabases"
  private val MODE_CONSTRAINT = "modeConstraint"
  private val TAGS = "tags"

  val VISIBLE_PERMITTED_OPTIONS = s"'$ALLOWED_DATABASES', '$DENIED_DATABASES', '$MODE_CONSTRAINT'"

  override def operation: String = "enable server"

  override def convert(map: MapValue, config: Option[Config]): ServerOptions = {
    map.foldLeft(ServerOptions(None, None, None, None)) {
      case (ops, (key, value)) =>
        if (key.equalsIgnoreCase(ALLOWED_DATABASES)) {
          value match {
            case list: ListValue =>
              val databases: Set[NormalizedDatabaseName] = list.iterator().asScala.map {
                case t: TextValue => new NormalizedDatabaseName(t.stringValue())
                case _ => throw new InvalidArgumentsException(
                    s"$ALLOWED_DATABASES expects a list of database names but got '$list'."
                  )
              }.toSet
              ops.copy(allowed = Some(databases))
            case t: TextValue =>
              ops.copy(allowed = Some(Set(new NormalizedDatabaseName(t.stringValue()))))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$ALLOWED_DATABASES expects a list of database names but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(DENIED_DATABASES)) {
          value match {
            case list: ListValue =>
              val databases: Set[NormalizedDatabaseName] = list.iterator().asScala.map {
                case t: TextValue => new NormalizedDatabaseName(t.stringValue())
                case _ => throw new InvalidArgumentsException(
                    s"$DENIED_DATABASES expects a list of database names but got '$list'."
                  )
              }.toSet
              ops.copy(denied = Some(databases))
            case t: TextValue =>
              ops.copy(denied = Some(Set(new NormalizedDatabaseName(t.stringValue()))))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$DENIED_DATABASES expects a list of database names but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(MODE_CONSTRAINT)) {
          value match {
            case t: TextValue =>
              val mode =
                try {
                  InstanceModeConstraint.valueOf(t.stringValue().toUpperCase(Locale.ROOT))
                } catch {
                  case _: Exception =>
                    throw new InvalidArgumentsException(
                      s"$MODE_CONSTRAINT expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '$value'."
                    )
                }
              ops.copy(mode = Some(mode))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$MODE_CONSTRAINT expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(TAGS)) {
          value match {
            case list: ListValue =>
              val tags: List[String] = list.iterator().asScala.map {
                case t: TextValue => t.stringValue()
                case _ => throw new InvalidArgumentsException(
                    s"$TAGS expects a list of tags but got '$list'."
                  )
              }.toList
              ops.copy(tags = Some(tags))
            case t: TextValue =>
              ops.copy(tags = Some(List(t.stringValue())))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$TAGS expects a list of tags names but got '$value'."
              )
          }
        } else {
          throw new InvalidArgumentsException(
            s"Unrecognised option '$key', expected $VISIBLE_PERMITTED_OPTIONS."
          )
        }
    }
  }
}

case class ServerOptions(
  allowed: Option[Set[NormalizedDatabaseName]],
  denied: Option[Set[NormalizedDatabaseName]],
  mode: Option[InstanceModeConstraint],
  tags: Option[List[String]]
)
