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
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

class OptionsConverterTest extends CypherFunSuite {

  val config: Config = Config.defaults()

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

  test("should throw exception if given invalid options for set") {
    the[InvalidArgumentsException] thrownBy AlterDatabaseOptionsConverter.convert(
      VirtualValues.map(Array("not an option"), Array(stringValue("Meh"))),
      Some(config)
    ) should have message "Could not alter database with unrecognised option(s): 'not an option'. Expected 'txLogEnrichment'."
  }

  test("should throw exception if given create-only options for set") {
    the[InvalidArgumentsException] thrownBy AlterDatabaseOptionsConverter.convert(
      VirtualValues.map(Array(ExistingDataOption.KEY), Array(stringValue("Use"))),
      Some(config)
    ) should have message "Could not alter database with 'CREATE DATABASE' option(s): 'existingData'. Expected 'txLogEnrichment'."
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

  for (opt <- Seq("tXlOgEnRiChMeNt", "txLogEnrichment", "txlogenrichment")) {
    test(s"alter should ignore casing of entry key for removal: $opt") {
      AlterDatabaseOptionsConverter.validForRemoval(
        Set("tXlOgEnRiChMeNt"),
        config
      ) shouldBe Set("txLogEnrichment")
    }
  }

  test("should throw exception if given invalid options for removal") {
    the[InvalidArgumentsException] thrownBy AlterDatabaseOptionsConverter.validForRemoval(
      Set("not an option"),
      config
    ) should have message "Could not alter database remove with unrecognised option(s): 'not an option'. Expected 'txLogEnrichment'."
  }

  test("should throw exception if given create-only options for removal") {
    the[InvalidArgumentsException] thrownBy AlterDatabaseOptionsConverter.validForRemoval(
      Set(ExistingDataOption.KEY),
      config
    ) should have message "Could not alter database remove with 'CREATE DATABASE' option(s): 'existingData'. Expected 'txLogEnrichment'."
  }
}
