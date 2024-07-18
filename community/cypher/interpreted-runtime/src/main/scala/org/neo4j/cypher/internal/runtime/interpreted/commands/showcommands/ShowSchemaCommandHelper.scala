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

import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.values.storable.DoubleArray
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.StringJoiner

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.MapHasAsScala

object ShowSchemaCommandHelper {
  def escapeBackticks(str: String): String = str.replace("`", "``")

  def createNodeIndexCommand(
    indexType: String,
    name: String,
    labelsOrTypes: List[String],
    properties: List[String],
    maybeOptionsMapString: Option[String] = None
  ): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
    createIndexCommand(
      indexType,
      name,
      s"(n$labelsOrTypesWithColons)",
      s"($escapedNodeProperties)",
      maybeOptionsMapString
    )
  }

  def createRelIndexCommand(
    indexType: String,
    name: String,
    labelsOrTypes: List[String],
    properties: List[String],
    maybeOptionsMapString: Option[String] = None
  ): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
    createIndexCommand(
      indexType,
      name,
      s"()-[r$labelsOrTypesWithColons]-()",
      s"($escapedRelProperties)",
      maybeOptionsMapString
    )
  }

  def createIndexCommand(
    indexType: String,
    name: String,
    nodeOrRelPattern: String,
    onDefinition: String,
    maybeOptionsMapString: Option[String] = None
  ): String = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val optionsString = maybeOptionsMapString.map(o => s" OPTIONS $o").getOrElse("")
    s"CREATE $indexType INDEX $escapedName FOR $nodeOrRelPattern ON $onDefinition$optionsString"
  }

  def createNodeConstraintCommand(
    name: String,
    labelsOrTypes: List[String],
    properties: List[String],
    predicate: String
  ): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    createConstraintCommand(name, s"(n$labelsOrTypesWithColons)", properties, propStringJoiner, predicate)
  }

  def createRelConstraintCommand(
    name: String,
    labelsOrTypes: List[String],
    properties: List[String],
    predicate: String
  ): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    createConstraintCommand(name, s"()-[r$labelsOrTypesWithColons]-()", properties, relPropStringJoiner, predicate)
  }

  private def createConstraintCommand(
    name: String,
    nodeOrRelPattern: String,
    properties: List[String],
    propJoiner: StringJoiner,
    predicate: String
  ): String = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedProperties = asEscapedString(properties, propJoiner)
    s"CREATE CONSTRAINT $escapedName FOR $nodeOrRelPattern REQUIRE ($escapedProperties) $predicate"
  }

  def extractOptionsMap(indexType: IndexType, provider: IndexProviderDescriptor, indexConfig: IndexConfig): MapValue = {
    val completedIndexConfig = indexType match {
      case IndexType.VECTOR =>
        val settingsValidator = VectorIndexVersion.fromDescriptor(provider).indexSettingValidator
        settingsValidator.trustIsValidToVectorIndexConfig(new IndexConfigAccessor(indexConfig)).config
      case _ => indexConfig
    }

    val (configKeys, configValues) = completedIndexConfig.asMap().asScala.toSeq.unzip
    val optionKeys = Array("indexConfig", "indexProvider")
    val optionValues =
      Array(VirtualValues.map(configKeys.toArray, configValues.toArray), Values.stringValue(provider.name))
    VirtualValues.map(optionKeys, optionValues)
  }

  def optionsAsString(providerString: String, configString: String): String = {
    s"{indexConfig: $configString, indexProvider: '$providerString'}"
  }

  def asEscapedString(list: List[String], stringJoiner: StringJoiner): String = {
    for (elem <- list) {
      stringJoiner.add(s"`${escapeBackticks(elem)}`")
    }
    stringJoiner.toString
  }

  def configAsString(indexConfig: IndexConfig, configValueAsString: Value => String): String = {
    val configString: StringJoiner = configStringJoiner
    val sortedIndexConfig = ListMap(indexConfig.asMap().asScala.toSeq.sortBy(_._1): _*)

    sortedIndexConfig.foldLeft(configString) { (acc, entry) =>
      val singleConfig: String = s"`${entry._1}`: ${configValueAsString(entry._2)}"
      acc.add(singleConfig)
    }
    configString.toString
  }

  def pointConfigValueAsString(configValue: Value): String = {
    configValue match {
      case doubleArray: DoubleArray => java.util.Arrays.toString(doubleArray.asObjectCopy)
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }

  private def colonStringJoiner = new StringJoiner(":", ":", "")
  def barStringJoiner = new StringJoiner("|", ":", "")
  def propStringJoiner = new StringJoiner(", n.", "n.", "")
  def relPropStringJoiner = new StringJoiner(", r.", "r.", "")
  private def configStringJoiner = new StringJoiner(",", "{", "}")
}
