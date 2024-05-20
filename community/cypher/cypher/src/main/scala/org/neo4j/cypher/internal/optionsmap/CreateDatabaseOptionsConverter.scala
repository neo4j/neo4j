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
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.virtual.MapValue

import java.util.Locale

import scala.jdk.CollectionConverters.IterableHasAsScala

case object CreateDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {

  // expectedKeys must be kept in sync with CreateDatabaseOptions below!
  val expectedKeys: Set[String] = Set(
    ExistingDataOption.KEY,
    ExistingSeedInstanceOption.KEY,
    StoreFormatOption.KEY,
    SeedURIOption.KEY,
    SeedCredentialsOption.KEY,
    SeedConfigOption.KEY,
    LogEnrichmentOption.KEY
  )

  val VISIBLE_PERMITTED_OPTIONS: String = expectedKeys.map(opt => s"'$opt'").mkString(", ")

  override def convert(optionsMap: MapValue, config: Option[Config]): CreateDatabaseOptions = {
    if (
      optionsMap.keySet().asScala.map(_.toLowerCase(Locale.ROOT)).toSeq.contains(
        LogEnrichmentOption.KEY.toLowerCase(Locale.ROOT)
      ) &&
      !config.exists(_.get(GraphDatabaseInternalSettings.change_data_capture))
    ) {
      throw new UnsupportedOperationException(s"${LogEnrichmentOption.KEY} is not supported yet")
    }
    val invalidKeys = optionsMap.keySet().asScala.toSeq.filterNot(found =>
      expectedKeys.exists(expected => found.equalsIgnoreCase(expected))
    )
    if (invalidKeys.nonEmpty) {
      throw new InvalidArgumentsException(
        s"Could not $operation with unrecognised option(s): ${invalidKeys.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    }

    // Keys must be kept in sync with expectedKeys above!
    CreateDatabaseOptions(
      existingData = ExistingDataOption.findIn(optionsMap, config),
      databaseSeed = ExistingSeedInstanceOption.findIn(optionsMap, config),
      storeFormatNewDb = StoreFormatOption.findIn(optionsMap, config),
      seedURI = SeedURIOption.findIn(optionsMap, config),
      seedCredentials = SeedCredentialsOption.findIn(optionsMap, config),
      seedConfig = SeedConfigOption.findIn(optionsMap, config),
      txLogEnrichment = LogEnrichmentOption.findIn(optionsMap, config)
    )
  }

  implicit override def operation: String = "create database"
}

case class CreateDatabaseOptions(
  existingData: Option[String],
  databaseSeed: Option[String],
  storeFormatNewDb: Option[String],
  seedURI: Option[String],
  seedCredentials: Option[String],
  seedConfig: Option[String],
  txLogEnrichment: Option[String]
)
