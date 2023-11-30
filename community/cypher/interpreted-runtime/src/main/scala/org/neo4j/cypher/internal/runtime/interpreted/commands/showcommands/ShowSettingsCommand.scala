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
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.defaultValueColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.descriptionColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.isDeprecatedColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.isDynamicColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.isExplicitlySetColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.startupValueColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.validValuesColumn
import org.neo4j.cypher.internal.ast.ShowSettingsClause.valueColumn
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
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends Command(columns, yieldColumns) {

  private def asMap[T](config: Config)(setting: SettingImpl[T]): Map[String, AnyValue] = requestedColumnsNames.map {
    case `nameColumn`         => nameColumn -> Values.of(setting.name())
    case `descriptionColumn`  => descriptionColumn -> Values.of(setting.description())
    case `valueColumn`        => valueColumn -> Values.of(setting.valueToString(config.get(setting)))
    case `isDynamicColumn`    => isDynamicColumn -> Values.of(setting.dynamic())
    case `defaultValueColumn` => defaultValueColumn -> Values.of(setting.valueToString(config.getDefault(setting)))
    case `startupValueColumn` =>
      startupValueColumn -> Values.of(setting.valueToString(config.getStartupValue(setting)))
    case `validValuesColumn`     => validValuesColumn -> Values.of(setting.validValues())
    case `isExplicitlySetColumn` => isExplicitlySetColumn -> Values.of(config.isExplicitlySet(setting))
    case `isDeprecatedColumn`    => isDeprecatedColumn -> Values.booleanValue(setting.deprecated)
    case unknown                 =>
      // This match should cover all existing columns but we get scala warnings
      // on non-exhaustive match due to it being string values
      throw new IllegalStateException(s"Missing case for column: $unknown")
  }.toMap[String, AnyValue]

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
      .toList
      .sortBy(_.name())
      .map(asMap(config)(_))

    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows)
    ClosingIterator.apply(updatedRows.iterator)
  }
}
