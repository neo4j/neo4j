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
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.virtual.MapValue

import java.util.Locale

import scala.jdk.CollectionConverters.IterableHasAsScala

case object AlterDatabaseOptionsConverter extends OptionsConverter[AlterDatabaseOptions] {

  // expectedKeys must be kept in sync with AlterDatabaseOptions below!
  private val expectedKeys: Map[String, String] = Map(
    LogEnrichmentOption.KEY.toLowerCase(Locale.ROOT) -> LogEnrichmentOption.KEY
  )

  private val VISIBLE_PERMITTED_OPTIONS: String = expectedKeys.values.map(opt => s"'$opt'").mkString(", ")

  def validForRemoval(keys: Set[String], config: Config): Set[String] = {
    if (keys.nonEmpty && !config.get(GraphDatabaseInternalSettings.change_data_capture)) {
      throw new UnsupportedOperationException("Removing options is not supported yet")
    }
    val (validKeys, invalidKeys) = keys.partition(key => expectedKeys.contains(key.toLowerCase(Locale.ROOT)))
    if (invalidKeys.nonEmpty) throwErrorForInvalidKeys(invalidKeys, s"$operation remove")
    validKeys.map(key => expectedKeys(key.toLowerCase(Locale.ROOT)))
  }

  override def convert(optionsMap: MapValue, config: Option[Config]): AlterDatabaseOptions = {
    if (optionsMap.nonEmpty && !config.exists(_.get(GraphDatabaseInternalSettings.change_data_capture))) {
      throw new UnsupportedOperationException("Setting options in alter is not supported yet")
    }
    val invalidKeys = optionsMap.keySet().asScala.toSeq.filterNot(found =>
      expectedKeys.contains(found.toLowerCase(Locale.ROOT))
    )
    if (invalidKeys.nonEmpty) throwErrorForInvalidKeys(invalidKeys, operation)

    // Keys must be kept in sync with expectedKeys above!
    AlterDatabaseOptions(
      txLogEnrichment = LogEnrichmentOption.findIn(optionsMap, config)
    )
  }

  private def throwErrorForInvalidKeys(invalidKeys: Iterable[String], operation: String) = {
    val validForCreateDatabase =
      invalidKeys.filter(invalidKey =>
        CreateDatabaseOptionsConverter.expectedKeys.map(_.toLowerCase(Locale.ROOT)).contains(
          invalidKey.toLowerCase(Locale.ROOT)
        )
      )

    if (validForCreateDatabase.isEmpty) {
      // keys are not even valid for CREATE DATABASE OPTIONS
      throw new InvalidArgumentsException(
        s"Could not $operation with unrecognised option(s): ${invalidKeys.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    } else {
      // keys are valid in CREATE DATABASE OPTIONS, but not allowed to be mutated through ALTER DATABASE SET OPTION
      throw new InvalidArgumentsException(
        s"Could not $operation with 'CREATE DATABASE' option(s): ${validForCreateDatabase.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    }
  }

  implicit override def operation: String = "alter database"

}

case class AlterDatabaseOptions(txLogEnrichment: Option[String])
