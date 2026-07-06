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
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption._
import org.neo4j.cypher.internal.options.OptionReader.Input
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CypherPlannerVersionOptionTest extends CypherFunSuite {

  private val emptyConfig: CypherConfiguration = CypherConfiguration.fromConfig(Config.defaults())

  test("plannerVersion values resolve successfully from input") {
    val reader = implicitly[OptionReader[CypherPlannerVersionOption]]

    def resolve(value: String): CypherPlannerVersionOption =
      reader.read(Input(emptyConfig, Set("plannerVersion" -> value))).result

    // Explicit values resolve to their corresponding case objects
    resolve("experimental") shouldBe experimental
    resolve("next") shouldBe next
    resolve("latest") shouldBe CypherPlannerVersionOption.latest
    resolve("v2026_05") shouldBe v2026_05
    resolve("v2026_04") shouldBe v2026_04
    // Retired versions resolve to the default planner instead of their own case object.
    resolve("v2026_03") shouldBe CypherPlannerVersionOption.default

    // The special token 'default' resolves to the default version
    resolve("default") shouldBe CypherPlannerVersionOption.default
  }

  test("render round-trips through reader for non-default, non-retired values") {
    val reader = implicitly[OptionReader[CypherPlannerVersionOption]]

    CypherPlannerVersionOption.values.foreach { v =>
      if (v != CypherPlannerVersionOption.default && !CypherPlannerVersionOption.retired.contains(v)) {
        val parsed = reader.read(Input(emptyConfig, Set("plannerVersion" -> v.name))).result
        parsed shouldBe v
      }
    }
  }

  test("All retired versions are still supported but map to the default planner") {
    for (retiredVersion <- CypherPlannerVersionOption.retired) {
      CypherPlannerVersionOption.supportedValues should contain(retiredVersion)
      CypherPlannerVersionOption.fromValue(retiredVersion.name) shouldBe CypherPlannerVersionOption.default
    }
  }

  test("isRetired is case-insensitive and only true for retired versions") {
    CypherPlannerVersionOption.isRetired("V2026_03") shouldBe true
    CypherPlannerVersionOption.isRetired("v2026_04") shouldBe false
    CypherPlannerVersionOption.isRetired("latest") shouldBe false
    CypherPlannerVersionOption.isRetired("experimental") shouldBe false
  }
}
