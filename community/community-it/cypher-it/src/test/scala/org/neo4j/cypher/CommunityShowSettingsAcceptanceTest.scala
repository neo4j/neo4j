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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.exceptions.SyntaxException

import java.lang.Boolean.FALSE

class CommunityShowSettingsAcceptanceTest extends ExecutionEngineFunSuite with ShowSettingsAcceptanceTestSupport {

  private def nullLastOrdering: Ordering[String] = {
    (x: String, y: String) =>
      (x, y) match {
        case (null, null) => 0
        case (_, null)    => -1
        case (null, _)    => +1
        case (lh, rh)     => lh.compareTo(rh)
      }
  }

  test("SHOW SETTING should throw appropriate error message when show_setting feature is disabled") {
    // GIVEN
    restartWithConfig(databaseConfig() ++ Map(GraphDatabaseInternalSettings.show_setting -> FALSE))

    (the[SyntaxException] thrownBy {
      execute("SHOW SETTINGS WHERE nonexistent = 'foo'")
    }).getMessage should startWith(
      "The `SHOW SETTINGS` clause is not available in this implementation of Cypher due to lack of support for show setting."
    )
  }

  test("show settings should return all settings") {
    // WHEN
    val result = execute("SHOW SETTINGS")
    // THEN
    assertContains(defaultColumnsOf(allSettings(graph)), result.toList)
  }

  test("show settings should return all settings on system") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    // WHEN
    val result = execute("SHOW SETTINGS")
    // THEN
    assertContains(defaultColumnsOf(allSettings(graph)), result.toList)
  }

  test("show settings should return all settings with yield *") {

    // WHEN
    val result = execute("SHOW SETTINGS YIELD *")
    // THEN
    assertContains(allSettings(graph), result.toList)
  }

  test("should show settings with explicit names once") {
    // WHEN
    val result = execute(s"SHOW SETTINGS ['db.format', 'dbms.routing_ttl', 'db.format']")

    // THEN
    result.size should be(2)
    result.toList should be(defaultColumnsOf(allSettings(graph)).filter(m =>
      m("name").equals("db.format") || m("name").equals("dbms.routing_ttl")
    ))
  }

  test("should show settings with full yield") {
    // WHEN
    val result =
      execute("SHOW SETTINGS YIELD name AS nm ORDER BY nm SKIP 20 LIMIT 30 WHERE nm CONTAINS 'db.' RETURN *")

    // THEN
    val expected = allSettings(graph)
      .sortBy(m => m("name").asInstanceOf[String])
      .slice(20, 50)
      .filter(_.get("name").map(_.asInstanceOf[String]).exists(_.contains("db.")))
      .map(row => Map("nm" -> row.get("name").orNull))
    result.size should be(30)
    result.toList should contain allElementsOf expected
  }

  test("should show settings with yield and order by") {
    // GIVEN
    implicit val ordering: Ordering[String] = nullLastOrdering
    // WHEN
    val result = execute("SHOW SETTINGS YIELD name, value ORDER BY value")

    // THEN
    // all settings should exist in result
    assertContains(allSettings(graph).map(m => m.view.filterKeys(Seq("name", "value").contains).toMap), result.toList)
    // result should be sorted by "value" (equivalent to it's sorted self)
    result.toList should contain theSameElementsInOrderAs result.toList.sortBy(
      _.get("value").orNull.asInstanceOf[String]
    )
  }

  test("should show settings with multiple order by") {
    // GIVEN
    implicit val ordering: Ordering[String] = nullLastOrdering
    // WHEN
    val result = execute("SHOW SETTINGS YIELD * ORDER BY name DESC RETURN name, value ORDER BY value ASC")

    // THEN
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("name DESC")
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("value ASC")

    val expected: Seq[Map[String, Any]] = allSettings(graph)
      .sortBy(entry => entry("name").asInstanceOf[String]).reverse // DESC
      .map(entry => Map("name" -> entry("name"), "value" -> entry("value"))) // RETURN name, value
      .sortBy(entry => entry("value").asInstanceOf[String]) // ASC
    result.toList should contain inOrderElementsOf expected
  }

  test("should be able to use old column name in ORDER BY and WHERE when renamed in YIELD") {
    // GIVEN
    val expectedSetting = allSettings(graph).head

    // WHEN
    // yield one column without renaming and three with renaming
    // use all four in order by and where,
    // one of renaming is just same name as before,
    // and for the others use the old name for one and new name for the last one
    val result = execute(
      """SHOW SETTINGS
        |YIELD name AS settingName, value, description AS explanation, isDynamic AS isDynamic
        |ORDER BY name, value, explanation, isDynamic
        |WHERE name = $settingParam AND value = $valueParam AND explanation IS NOT NULL AND isDynamic IS :: BOOLEAN
        |RETURN settingName""".stripMargin,
      Map("settingParam" -> expectedSetting("name"), "valueParam" -> expectedSetting("value"))
    ).toList

    // THEN
    result should be(List(Map("settingName" -> expectedSetting("name"))))
  }

  test("should not show enterprise settings in community") {
    // WHEN
    val result = execute(
      "SHOW SETTINGS 'browser.allow_outgoing_connections' YIELD name, startupValue"
    )

    // THEN
    result.size should be(0)
  }

  test("should show explicit settings with yield") {
    // WHEN
    val result = execute(
      "SHOW SETTINGS ['db.format', 'dbms.routing_ttl'] YIELD name, startupValue"
    )

    // THEN
    result.size should be(2)
    val expected = allSettings(graph)
      .filter(m =>
        Seq("db.format", "dbms.routing_ttl").contains(m("name"))
      )
      .map(m =>
        m.view.filterKeys(k => Seq("name", "startupValue").contains(k)).toMap
      )
    assertContains(expected, result.toList)
  }

  test("should show settings with where") {
    // WHEN
    val result = execute(s"SHOW SETTINGS WHERE name = 'db.format'")

    // THEN
    result.size should be(1)
    result.toList should be(defaultColumnsOf(allSettings(graph)).filter(m =>
      m("name").equals("db.format")
    ))
  }

  // Planner tests

  test("show settings plan") {
    // WHEN
    val result = execute("EXPLAIN SHOW SETTINGS")

    // THEN
    result.executionPlanString() should include("allSettings, defaultColumns")
  }

  test("show settings plan on system") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    // WHEN
    val result = execute("EXPLAIN SHOW SETTINGS")

    // THEN
    result.executionPlanString() should include("AdministrationCommand")
    result.executionPlanString() should not include "settingsMatching(foo), defaultColumns"
  }

}
