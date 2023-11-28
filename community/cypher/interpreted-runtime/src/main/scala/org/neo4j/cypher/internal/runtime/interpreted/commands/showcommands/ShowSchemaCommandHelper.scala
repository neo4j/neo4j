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

  def extractOptionsMap(providerName: String, indexConfig: IndexConfig): MapValue = {
    val (configKeys, configValues) = indexConfig.asMap().asScala.toSeq.unzip
    val optionKeys = Array("indexConfig", "indexProvider")
    val optionValues =
      Array(VirtualValues.map(configKeys.toArray, configValues.toArray), Values.stringValue(providerName))
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

  def colonStringJoiner = new StringJoiner(":", ":", "")
  def barStringJoiner = new StringJoiner("|", ":", "")
  def propStringJoiner = new StringJoiner(", n.", "n.", "")
  def relPropStringJoiner = new StringJoiner(", r.", "r.", "")
  private def configStringJoiner = new StringJoiner(",", "{", "}")
}
