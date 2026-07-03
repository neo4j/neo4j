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
package org.neo4j.cypher.internal.options

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CypherConfigurationTest extends CypherFunSuite {

  test("toggledFeatures should add features if set to true in config") {
    val config = Config.defaults(GraphDatabaseInternalSettings.show_setting, java.lang.Boolean.TRUE)

    val features: Set[String] = CypherConfiguration.fromConfig(config).toggledFeatures(
      defaultFeatures = Seq("semanticTestFeature1", "semanticTestFeature2"),
      features = (GraphDatabaseInternalSettings.show_setting, "semanticTestFeature3")
    )

    features shouldBe Set("semanticTestFeature1", "semanticTestFeature2", "semanticTestFeature3")
  }

  test("toggledFeatures should not add features if set to false in config") {
    val config = Config.defaults(GraphDatabaseInternalSettings.show_setting, java.lang.Boolean.FALSE)

    val features: Set[String] = CypherConfiguration.fromConfig(config).toggledFeatures(
      defaultFeatures = Seq("semanticTestFeature1", "semanticTestFeature2"),
      features = (GraphDatabaseInternalSettings.show_setting, "semanticTestFeature3")
    )

    features shouldBe Set("semanticTestFeature1", "semanticTestFeature2")
  }

  test("toggledFeatures should filter out defaults if they are set to false in config") {
    val config = Config.defaults(GraphDatabaseInternalSettings.show_setting, java.lang.Boolean.FALSE)

    val features: Set[String] = CypherConfiguration.fromConfig(config).toggledFeatures(
      defaultFeatures = Seq("semanticTestFeature1", "semanticTestFeature2"),
      features = (GraphDatabaseInternalSettings.show_setting, "semanticTestFeature1")
    )

    features shouldBe Set("semanticTestFeature2")
  }

  test("toggledFeatures should not filter out defaults if they are set to true in config") {
    val config = Config.defaults(GraphDatabaseInternalSettings.show_setting, java.lang.Boolean.TRUE)

    val features: Set[String] = CypherConfiguration.fromConfig(config).toggledFeatures(
      defaultFeatures = Seq("semanticTestFeature1", "semanticTestFeature2"),
      features = (GraphDatabaseInternalSettings.show_setting, "semanticTestFeature1")
    )

    features shouldBe Set("semanticTestFeature1", "semanticTestFeature2")
  }
}
