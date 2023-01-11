/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

class OptionsConverterTest extends CypherFunSuite {

  val config: Config = Config.defaults()
  config.set(GraphDatabaseInternalSettings.change_data_capture, java.lang.Boolean.TRUE)

  test("create should ignore casing of entry key") {
    CreateDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("ExIsTiNgDaTa"), Array(stringValue("use"))),
      Some(config)
    ).existingData shouldBe Some("use")
  }

  test("create should parse existingData option value to lowercase regardless of input casing") {
    // legacy, behaviour is different from other options (they keep casing unchanged)
    CreateDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("existingData"), Array(stringValue("USE"))),
      Some(config)
    ).existingData shouldBe Some("use")
    CreateDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("existingData"), Array(stringValue("Use"))),
      Some(config)
    ).existingData shouldBe Some("use")
  }

  test("alter should ignore casing of entry key") {
    AlterDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("tXlOgEnRiChMeNt"), Array(stringValue("FULL"))),
      Some(config)
    ).txLogEnrichment shouldBe Some("FULL")
  }

  test("alter should parse txLogEnrichment option value to uppercase regardless of input casing") {
    // behaviour is different from other options (they keep casing unchanged)
    AlterDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("txLogEnrichment"), Array(stringValue("full"))),
      Some(config)
    ).txLogEnrichment shouldBe Some("FULL")
    AlterDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("txLogEnrichment"), Array(stringValue("DiFf"))),
      Some(config)
    ).txLogEnrichment shouldBe Some("DIFF")
  }
}
