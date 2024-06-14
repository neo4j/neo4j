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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingImpl
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting

import java.lang.Boolean.TRUE

import scala.jdk.CollectionConverters.MapHasAsScala

trait ShowSettingsAcceptanceTestSupport extends GraphDatabaseTestSupport {
  self: CypherFunSuite =>

  abstract override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE
  )

  private def setting(
    name: String,
    description: String,
    value: AnyRef,
    isDynamic: Boolean,
    defaultValue: String,
    startupValue: String,
    validValues: String,
    isExplicitlySet: Boolean,
    deprecated: Boolean
  ): Map[String, AnyRef] = Map(
    "name" -> name,
    "description" -> description,
    "value" -> value,
    "isDynamic" -> isDynamic.asInstanceOf[AnyRef],
    "defaultValue" -> defaultValue,
    "startupValue" -> startupValue,
    "validValues" -> validValues,
    "isExplicitlySet" -> isExplicitlySet.asInstanceOf[AnyRef],
    "isDeprecated" -> deprecated.asInstanceOf[AnyRef]
  )

  protected def allSettings(graph: GraphDatabaseCypherService): Seq[Map[String, AnyRef]] = {
    val config = graph.config
    config
      .getDeclaredSettings.asScala
      .values
      .map(_.asInstanceOf[SettingImpl[AnyRef]])
      .filterNot(_.internal())
      .toSeq
      .sortBy(_.name())
      .map { s =>
        setting(
          name = s.name(),
          description = s.description(),
          value = s.valueToString(config.get(s)),
          isDynamic = s.dynamic(),
          defaultValue = s.valueToString(config.getDefault(s)),
          startupValue = s.valueToString(config.getStartupValue(s)),
          validValues = s.validValues(),
          config.isExplicitlySet(s),
          s.deprecated()
        )
      }
  }

  private def defaultColumns(setting: Map[String, Any]): Map[String, Any] = {
    val defaultCols = Seq("name", "description", "value", "isDynamic", "defaultValue")
    setting.view.filterKeys(defaultCols.contains).toMap
  }

  protected def defaultColumnsOf(rows: Seq[Map[String, Any]]): Seq[Map[String, Any]] = rows.map(defaultColumns)

  protected def assertContains(expected: Seq[Map[String, Any]], actual: List[Map[String, Any]]): Unit = {
    actual.size should be(expected.size)
    expected.foreach(expectedRow => {
      val name = expectedRow.get("name")
      val resultRow = actual.find(_.get("name").equals(name))
      withClue(s"Looking for $name") {
        resultRow should not be empty
        resultRow.get should be(expectedRow)
      }
    })
  }

}
