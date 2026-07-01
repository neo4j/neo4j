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
package org.neo4j.cypher.internal.runtime.admin.topology

import org.neo4j.cypher.internal.ast.ShowDatabase.PROPERTY_SHARD_REPLICA_ROLE
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

case class ShowDatabaseResult(
  details: DatabaseDetails,
  default: Boolean,
  home: Boolean,
  constituents: Seq[String],
  aliases: Seq[String],
  graphShards: Option[Seq[String]],
  propertyShards: Option[Seq[String]] = None
) {
  private val isPropertyShard: Boolean = details.databaseType().equals(DatabaseDetails.TYPE_PROPERTY_SHARD)

  private val isVirtual: Boolean =
    details.databaseType().equals(DatabaseDetails.TYPE_COMPOSITE) ||
      details.databaseType().equals(DatabaseDetails.TYPE_GRAPH_ENGINE)

  def roleValue(): AnyValue = {
    if (isPropertyShard) {
      Values.stringValue(PROPERTY_SHARD_REPLICA_ROLE)
    } else {
      details.role().map[AnyValue](s => Values.stringValue(s)).orElse(Values.NO_VALUE)
    }
  }

  def requestedPrimariesCountValue(): Value = {
    if (details.requestedPrimariesCount() == null || isPropertyShard) {
      Values.NO_VALUE
    } else {
      Values.longValue(details.requestedPrimariesCount().intValue())
    }
  }

  def currentPrimariesCountValue(): Value = {
    if (details.actualPrimariesCount() == null || isPropertyShard || isVirtual) {
      Values.NO_VALUE
    } else {
      Values.longValue(details.actualPrimariesCount().intValue())
    }
  }

  def requestedSecondariesCountValue(): Value = {
    if (details.requestedSecondariesCount() == null || isPropertyShard) {
      Values.NO_VALUE
    } else {
      Values.longValue(details.requestedSecondariesCount().intValue())
    }
  }

  def currentSecondariesCountValue(): Value = {
    if (details.actualSecondariesCount() == null || isPropertyShard || isVirtual) {
      Values.NO_VALUE
    } else {
      Values.longValue(details.actualSecondariesCount().intValue())
    }
  }

  def requestedPropertyShardsReplicaCountValue(): Value = {
    if (details.requestedSecondariesCount() != null && isPropertyShard) {
      Values.longValue(
        details.requestedSecondariesCount().intValue()
      )
    } else {
      Values.NO_VALUE
    }
  }

  def currentPropertyShardsReplicaCountValue(): Value = {
    if (details.actualSecondariesCount() != null && isPropertyShard) {
      Values.longValue(
        details.actualSecondariesCount().intValue()
      )
    } else {
      Values.NO_VALUE
    }
  }

  def storeValue(): AnyValue = {
    if (isVirtual) Values.NO_VALUE
    else details.readableStoreId().map[AnyValue](s => Values.stringValue(s)).orElse(
      Values.NO_VALUE
    )
  }

  def optionsValue(): AnyValue =
    if (isVirtual) {
      Values.NO_VALUE
    } else {
      val valueOptions =
        details.options().asScala.view.mapValues(v => ValueUtils.of(v)).toMap[String, AnyValue].asJava
      VirtualValues.fromMap(valueOptions, valueOptions.size, 0)
    }

}
