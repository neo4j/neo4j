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
package org.neo4j.cypher.internal.administration

import org.neo4j.cypher.internal.ast.ShowDatabase.ACCESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ADDRESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DATABASE_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_COMMITTED_TX_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.NAME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.OPTIONS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ROLE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SERVER_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STATUS_MSG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.TYPE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.WRITER_COL
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

object DatabaseDetailsMapper {

  def toMapValue(
    databaseDetails: DatabaseDetails
  ): AnyValue = {
    VirtualValues.map(
      Array(
        NAME_COL,
        TYPE_COL,
        ACCESS_COL,
        ADDRESS_COL,
        ROLE_COL,
        WRITER_COL,
        CURRENT_STATUS_COL,
        STATUS_MSG_COL,
        DATABASE_ID_COL,
        SERVER_ID_COL,
        CURRENT_PRIMARIES_COUNT_COL,
        CURRENT_SECONDARIES_COUNT_COL,
        STORE_COL,
        LAST_COMMITTED_TX_COL,
        REPLICATION_LAG_COL,
        OPTIONS_COL
      ),
      Array(
        Values.stringValue(databaseDetails.namedDatabaseId().name()),
        Values.stringValue(databaseDetails.databaseType()),
        Values.stringValue(databaseDetails.databaseAccess().getStringRepr),
        databaseDetails.boltAddress().map[AnyValue](s => Values.stringValue(s.toString)).orElse(Values.NO_VALUE),
        databaseDetails.role().map[AnyValue](s => Values.stringValue(s)).orElse(Values.NO_VALUE),
        Values.booleanValue(databaseDetails.writer()),
        Values.stringValue(databaseDetails.status()),
        Values.stringValue(databaseDetails.statusMessage()),
        databaseDetails.readableExternalStoreId().map[AnyValue](s => Values.stringValue(s)).orElse(Values.NO_VALUE),
        databaseDetails.serverId().map[AnyValue](s => Values.stringValue(s.uuid().toString)).orElse(Values.NO_VALUE),
        Values.longValue(databaseDetails.actualPrimariesCount()),
        Values.longValue(databaseDetails.actualSecondariesCount()),
        databaseDetails.readableStoreId().map[AnyValue](s => Values.stringValue(s)).orElse(Values.NO_VALUE),
        databaseDetails.lastCommittedTxId().map[AnyValue](s => Values.longValue(s)).orElse(Values.NO_VALUE),
        databaseDetails.txCommitLag().map[AnyValue](s => Values.longValue(s)).orElse(Values.NO_VALUE), {
          val valueOptions =
            databaseDetails.options().asScala.view.mapValues(v => Values.stringValue(v)).toMap[String, AnyValue].asJava
          VirtualValues.fromMap(valueOptions, valueOptions.size, 0)
        }
      )
    )
  }
}
