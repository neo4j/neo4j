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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.configuration.Config
import org.neo4j.configuration.SettingImpl
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ParameterFromSlot
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.kernel.impl.index.schema.config.CrsConfig
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

class ShowSettingsCommandTest extends ShowCommandTestBase {

  private val defaultColumns =
    ShowSettingsClause(Left(List.empty), None, List.empty, yieldAll = false)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val allColumns =
    ShowSettingsClause(Left(List.empty), None, List.empty, yieldAll = true)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val config = Config.defaults()

  private val allNonInternalSettings =
    config.getDeclaredSettings.values().asScala
      .flatMap(setting => Try(setting.asInstanceOf[SettingImpl[_]]).toOption)
      .filterNot(_.internal())
      .toList
      .sortBy(_.name())
      .map(asMap(_))

  private def asMap[T](setting: SettingImpl[T]): Map[String, Any] = {
    Map(
      "name" -> setting.name(),
      "value" -> setting.valueToString(config.get(setting)),
      "isDynamic" -> setting.dynamic(),
      "defaultValue" -> setting.valueToString(config.getDefault(setting)),
      "description" -> setting.description(),
      "startupValue" -> setting.valueToString(config.getStartupValue(setting)),
      "isExplicitlySet" -> config.isExplicitlySet(setting),
      "validValues" -> setting.validValues(),
      "deprecated" -> setting.deprecated()
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Defaults:
    when(ctx.getConfig).thenReturn(config)
    when(ctx.transactionalContext).thenReturn(txContext)
    when(txContext.securityContext).thenReturn(securityContext)
    when(securityContext.mode()).thenReturn(AccessMode.Static.FULL)
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    name: Option[String],
    value: Option[String] = None,
    isDynamic: Option[Boolean] = None,
    defaultValue: Option[String] = None,
    description: Option[String] = None,
    startupValue: Option[String] = None,
    isExplicitlySet: Option[Boolean] = None,
    validValues: Option[String] = None,
    isDeprecated: Option[Boolean] = None
  ): Unit = {
    name.foreach(expected => resultMap(ShowSettingsClause.nameColumn) should be(Values.stringValue(expected)))
    value.foreach(expected => resultMap(ShowSettingsClause.valueColumn) should be(Values.stringOrNoValue(expected)))
    isDynamic.foreach(expected =>
      resultMap(ShowSettingsClause.isDynamicColumn) should be(Values.booleanValue(expected))
    )
    defaultValue.foreach(expected =>
      resultMap(ShowSettingsClause.defaultValueColumn) should be(Values.stringOrNoValue(expected))
    )
    description.foreach(expected =>
      resultMap(ShowSettingsClause.descriptionColumn) should be(Values.stringValue(expected))
    )
    startupValue.foreach(expected =>
      resultMap(ShowSettingsClause.startupValueColumn) should be(Values.stringOrNoValue(expected))
    )
    isExplicitlySet.foreach(expected =>
      resultMap(ShowSettingsClause.isExplicitlySetColumn) should be(Values.booleanValue(expected))
    )
    validValues.foreach(expected =>
      resultMap(ShowSettingsClause.validValuesColumn) should be(Values.stringValue(expected))
    )
    isDeprecated.foreach(expected =>
      resultMap(ShowSettingsClause.isDeprecatedColumn) should be(Values.booleanValue(expected))
    )
  }

  // Tests

  test("show settings should give back correct default values") {
    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size allNonInternalSettings.size
    result.zipWithIndex.foreach { case (res, index) =>
      val expectedSetting = allNonInternalSettings(index)
      checkResult(
        res,
        name = expectedSetting("name").asInstanceOf[String],
        value = expectedSetting("value").asInstanceOf[String],
        isDynamic = expectedSetting("isDynamic").asInstanceOf[Boolean],
        defaultValue = expectedSetting("defaultValue").asInstanceOf[String],
        description = expectedSetting("description").asInstanceOf[String]
      )
    }
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List(
        ShowSettingsClause.startupValueColumn,
        ShowSettingsClause.isExplicitlySetColumn,
        ShowSettingsClause.validValuesColumn,
        ShowSettingsClause.isDeprecatedColumn
      )
    })
  }

  test("show settings should give back correct full values") {
    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), allColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size allNonInternalSettings.size
    result.zipWithIndex.foreach { case (res, index) =>
      val expectedSetting = allNonInternalSettings(index)
      checkResult(
        res,
        name = expectedSetting("name").asInstanceOf[String],
        value = expectedSetting("value").asInstanceOf[String],
        isDynamic = expectedSetting("isDynamic").asInstanceOf[Boolean],
        defaultValue = expectedSetting("defaultValue").asInstanceOf[String],
        description = expectedSetting("description").asInstanceOf[String],
        startupValue = expectedSetting("startupValue").asInstanceOf[String],
        isExplicitlySet = expectedSetting("isExplicitlySet").asInstanceOf[Boolean],
        validValues = expectedSetting("validValues").asInstanceOf[String],
        isDeprecated = expectedSetting("deprecated").asInstanceOf[Boolean]
      )
    }
  }

  test("show settings should return the settings sorted on name") {
    // Make sure the input isn't sorted on name
    val mixedSettings = config.getDeclaredSettings.values().asScala
      .flatMap(setting => Try(setting.asInstanceOf[SettingImpl[_]]).toOption)
      .filterNot(_.internal())
      .toList
      .sortBy(_.description()) // sort on something else than name
      .map(setting => (setting.name(), setting))
      .toMap
      .asInstanceOf[Map[String, Setting[AnyRef]]]

    val mockConfig = mock[Config]
    when(ctx.getConfig).thenReturn(mockConfig)
    when(mockConfig.getDeclaredSettings).thenReturn(mixedSettings.asJava)

    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size allNonInternalSettings.size
    result.zipWithIndex.foreach { case (res, index) =>
      val expectedSetting = allNonInternalSettings(index)
      checkResult(res, name = expectedSetting("name").asInstanceOf[String])
    }
  }

  test("show settings should not return internal settings") {
    // Mock which settings to return to an explicitly internal one:
    // The internal settings in GraphDatabaseInternalSettings get marked as internal later and cannot be used here :(
    // This setting was one of few that was marked internal when created
    val internalSetting = CrsConfig.group(CoordinateReferenceSystem.CARTESIAN).max
    val mockConfig = mock[Config]
    when(mockConfig.getDeclaredSettings).thenReturn(Map(
      internalSetting.name() -> internalSetting
    ).asInstanceOf[Map[String, Setting[AnyRef]]].asJava)
    when(ctx.getConfig).thenReturn(mockConfig)

    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 0
  }

  test("show settings should return deprecated settings") {
    // Given (can't reach any actual deprecated settings as no community settings are deprecated)
    val settingName = "setting.deprecated"
    val mockSetting = mock[SettingImpl[Boolean]]
    when(mockSetting.name()).thenReturn(settingName)
    when(mockSetting.deprecated()).thenReturn(true)
    when(mockSetting.internal()).thenReturn(false)
    when(mockSetting.dynamic()).thenReturn(false)
    when(mockSetting.description()).thenReturn("I'm deprecated")
    when(mockSetting.validValues()).thenReturn("true, false")
    when(mockSetting.valueToString(any())).thenReturn("false")
    val settingsMap: Map[String, Setting[AnyRef]] = Map(settingName -> mockSetting.asInstanceOf[Setting[AnyRef]])

    val mockConfig = mock[Config]
    when(mockConfig.getDeclaredSettings).thenReturn(settingsMap.asJava)
    when(ctx.getConfig).thenReturn(mockConfig)

    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), allColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(result.head, name = settingName, isDeprecated = true)
  }

  test("show settings should only return specified settings when given names (string list)") {
    // Given
    // sort settings on something else than name,
    // then pick first and last 5 settings to just get some random ones
    val unsortedSettings = allNonInternalSettings.sortBy(setting => setting("description").asInstanceOf[String])
    val wantedSettings = unsortedSettings.take(5) ++ unsortedSettings.takeRight(5)
    val wantedSettingNames = wantedSettings.map(setting => setting("name").asInstanceOf[String])

    // When
    val showSettings = ShowSettingsCommand(Left(wantedSettingNames), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    val sortedSettingNames = wantedSettingNames.sorted
    result should have size sortedSettingNames.size
    result.zipWithIndex.foreach { case (res, index) =>
      checkResult(res, name = sortedSettingNames(index))
    }
  }

  test("show settings should only return specified settings when given names (string expression)") {
    // Given
    // pick random setting
    val wantedSetting = allNonInternalSettings.slice(42, 43).head("name").asInstanceOf[String]
    val wantedSettingExpression = ParameterFromSlot(0, "settingName")
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(Values.stringValue(wantedSetting)))

    // When
    val showSettings = ShowSettingsCommand(Right(wantedSettingExpression), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryStateWithParams, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(result.head, name = wantedSetting)
  }

  test("show settings should only return allowed settings") {
    // Given
    val accessMode = mock[AccessMode]
    when(accessMode.allowsShowSetting(any())).thenAnswer(invocation =>
      invocation.getArgument(0).asInstanceOf[String] match {
        case s if s.startsWith("dbms.") => PermissionState.EXPLICIT_GRANT
        case s if s.startsWith("db.")   => PermissionState.EXPLICIT_DENY
        case _                          => PermissionState.NOT_GRANTED
      }
    )
    when(securityContext.mode()).thenReturn(accessMode)

    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    val expectedSettings = allNonInternalSettings.map(setting => setting("name").asInstanceOf[String])
      .filter(setting => setting.startsWith("dbms."))

    result should have size expectedSettings.size
    result.zipWithIndex.foreach { case (res, index) =>
      checkResult(res, name = expectedSettings(index))
    }
  }

  test("show settings should rename columns renamed in YIELD") {
    // Given: YIELD name AS setting, startupValue AS startupVal, value, description
    val yieldColumns: List[CommandResultItem] = List(
      CommandResultItem(ShowSettingsClause.nameColumn, Variable("setting")(InputPosition.NONE))(InputPosition.NONE),
      CommandResultItem(
        ShowSettingsClause.startupValueColumn,
        Variable("startupVal")(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowSettingsClause.valueColumn,
        Variable(ShowSettingsClause.valueColumn)(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowSettingsClause.descriptionColumn,
        Variable(ShowSettingsClause.descriptionColumn)(InputPosition.NONE)
      )(InputPosition.NONE)
    )

    // When
    val showSettings = ShowSettingsCommand(Left(List.empty), allColumns, yieldColumns)
    val result = showSettings.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size allNonInternalSettings.size
    result.zipWithIndex.foreach { case (res, index) =>
      val expectedSetting = allNonInternalSettings(index)
      res should be(Map(
        "setting" -> Values.stringValue(expectedSetting("name").asInstanceOf[String]),
        "startupVal" -> Values.stringOrNoValue(expectedSetting("startupValue").asInstanceOf[String]),
        ShowSettingsClause.valueColumn -> Values.stringOrNoValue(expectedSetting("value").asInstanceOf[String]),
        ShowSettingsClause.descriptionColumn -> Values.stringValue(expectedSetting("description").asInstanceOf[String])
      ))
    }
  }
}
