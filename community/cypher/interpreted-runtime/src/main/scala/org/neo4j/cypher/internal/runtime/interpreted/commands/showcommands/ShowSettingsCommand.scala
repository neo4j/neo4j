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

import org.neo4j.configuration.Config
import org.neo4j.configuration.SettingImpl
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

// SHOW SETTING[S] [names | nameExpression] [WHERE clause | YIELD clause]
case class ShowSettingsCommand(
  givenNames: Either[List[String], Expression],
  verbose: Boolean,
  columns: List[ShowColumn]
) extends Command(columns) {

  private def asMap[T](config: Config)(setting: SettingImpl[T]): Map[String, AnyValue] = {
    val defaultColumns = Map(
      "name" -> Values.of(setting.name()),
      "description" -> Values.of(setting.description()),
      "value" -> Values.of(setting.valueToString(config.get(setting))),
      "isDynamic" -> Values.of(setting.dynamic()),
      "defaultValue" -> Values.of(setting.valueToString(config.getDefault(setting)))
    )
    lazy val verboseColumns = Map(
      "startupValue" -> Values.of(setting.valueToString(config.getStartupValue(setting))),
      "validValues" -> Values.of(setting.validValues()),
      "isExplicitlySet" -> Values.of(config.isExplicitlySet(setting)),
      "isDeprecated" -> Values.booleanValue(setting.deprecated)
    )
    if (verbose) defaultColumns ++ verboseColumns
    else defaultColumns
  }

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val names = Command.extractNames(givenNames, state, baseRow)
    val config = state.query.getConfig
    val txContext = state.query.transactionalContext
    val accessMode = txContext.securityContext.mode()

    val rows = config.getDeclaredSettings.values().asScala
      .filter(s => names.isEmpty || names.contains(s.name()))
      .flatMap(setting => Try(setting.asInstanceOf[SettingImpl[_]]).toOption)
      .filterNot(_.internal())
      .filter(setting => accessMode.allowsShowSetting(setting.name()).allowsAccess())
      .toSeq
      .sortBy(_.name())
      .map(asMap(config)(_))

    ClosingIterator.apply(rows.iterator)
  }
}
