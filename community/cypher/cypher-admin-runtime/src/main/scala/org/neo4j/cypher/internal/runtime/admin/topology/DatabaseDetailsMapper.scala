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

import org.neo4j.cypher.internal.ast.ShowDatabase.ACCESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ADDRESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ALIASES_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CONSTITUENTS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CREATION_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PROPERTY_SHARD_REPLICA_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DATABASE_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DEFAULT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DEFAULT_LANGUAGE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.GRAPH_SHARDS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.HOME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_COMMITTED_TX_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_START_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_STOP_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.NAME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.OPTIONS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.PROPERTY_SHARDS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_PROPERTY_SHARDS_REPLICA_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ROLE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SERVER_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SHARD_TX_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STATUS_MSG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.TYPE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.WRITER_COL
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder

object DatabaseDetailsMapper {

  def toMap(
    showDatabaseResult: ShowDatabaseResult
  ): Map[String, AnyValue] = {
    val databaseDetails: DatabaseDetails = showDatabaseResult.details
    val constituentValue = buildStringListValue(showDatabaseResult.constituents)
    val aliasesValue = buildStringListValue(showDatabaseResult.aliases.sorted)
    val graphShardValue = showDatabaseResult.graphShards.map(buildStringListValue).getOrElse(Values.NO_VALUE)
    val propertyShardValue = showDatabaseResult.propertyShards.map(buildStringListValue).getOrElse(Values.NO_VALUE)

    Map(
      NAME_COL -> Values.stringValue(databaseDetails.namedDatabaseId().name()),
      TYPE_COL -> Values.stringValue(databaseDetails.databaseType()),
      ACCESS_COL -> Values.stringValue(databaseDetails.databaseAccess().getStringRepr),
      ADDRESS_COL -> databaseDetails.boltAddress().map[AnyValue](s => Values.stringValue(s.toString)).orElse(
        Values.NO_VALUE
      ),
      ROLE_COL -> showDatabaseResult.roleValue(),
      WRITER_COL -> Values.booleanValue(databaseDetails.writer()),
      DEFAULT_COL -> Values.booleanValue(showDatabaseResult.default),
      HOME_COL -> Values.booleanValue(showDatabaseResult.home),
      REQUESTED_STATUS_COL -> Values.stringValue(databaseDetails.requestedStatus()),
      CURRENT_STATUS_COL -> Values.stringValue(databaseDetails.actualStatus()),
      STATUS_MSG_COL -> Values.stringValue(databaseDetails.statusMessage()),
      DATABASE_ID_COL -> databaseDetails.readableExternalStoreId().map[AnyValue](s => Values.stringValue(s)).orElse(
        Values.NO_VALUE
      ),
      SERVER_ID_COL -> databaseDetails.serverId().map[AnyValue](s => Values.stringValue(s.uuid().toString)).orElse(
        Values.NO_VALUE
      ),
      REQUESTED_PRIMARIES_COUNT_COL -> showDatabaseResult.requestedPrimariesCountValue(),
      CURRENT_PRIMARIES_COUNT_COL -> showDatabaseResult.currentPrimariesCountValue(),
      REQUESTED_SECONDARIES_COUNT_COL -> showDatabaseResult.requestedSecondariesCountValue(),
      CURRENT_SECONDARIES_COUNT_COL -> showDatabaseResult.currentSecondariesCountValue(),
      REQUESTED_PROPERTY_SHARDS_REPLICA_COUNT_COL -> showDatabaseResult.requestedPropertyShardsReplicaCountValue(),
      CURRENT_PROPERTY_SHARD_REPLICA_COUNT_COL -> showDatabaseResult.currentPropertyShardsReplicaCountValue(),
      STORE_COL -> showDatabaseResult.storeValue(),
      LAST_COMMITTED_TX_COL -> (if (databaseDetails.lastCommittedTxId().isPresent)
                                  Values.longValue(databaseDetails.lastCommittedTxId().getAsLong)
                                else Values.NO_VALUE),
      REPLICATION_LAG_COL -> (if (databaseDetails.txCommitLag().isPresent)
                                Values.longValue(databaseDetails.txCommitLag().getAsLong)
                              else Values.NO_VALUE),
      SHARD_TX_LAG_COL -> (if (databaseDetails.shardCommitLag().isPresent)
                             Values.longValue(databaseDetails.shardCommitLag().getAsLong)
                           else Values.NO_VALUE),
      OPTIONS_COL -> showDatabaseResult.optionsValue(),
      CONSTITUENTS_COL -> constituentValue,
      ALIASES_COL -> aliasesValue,
      GRAPH_SHARDS_COL -> graphShardValue,
      PROPERTY_SHARDS_COL -> propertyShardValue,
      CREATION_TIME_COL -> databaseDetails.creationTime().map[AnyValue](d => DateTimeValue.datetime(d)).orElse(
        Values.NO_VALUE
      ),
      LAST_START_TIME_COL -> databaseDetails.lastStartTime().map[AnyValue](d => DateTimeValue.datetime(d)).orElse(
        Values.NO_VALUE
      ),
      LAST_STOP_TIME_COL -> databaseDetails.lastStopTime().map[AnyValue](d => DateTimeValue.datetime(d)).orElse(
        Values.NO_VALUE
      ),
      DEFAULT_LANGUAGE_COL -> databaseDetails.cypherVersion().map[AnyValue](s =>
        Values.stringValue(s.description)
      ).orElse(Values.NO_VALUE)
    )
  }

  private def buildStringListValue(strings: Seq[String]): ListValue = {
    val lvb = ListValueBuilder.newListBuilder()
    strings.foreach(const => lvb.add(Values.stringValue(const)))
    lvb.build()
  }
}
