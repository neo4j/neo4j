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
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ROLE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SERVER_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STATUS_MSG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.TYPE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.WRITER_COL
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseInfo
import org.neo4j.dbms.database.ExtendedDatabaseInfo
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.OptionConverters.RichOptional

trait DatabaseInfoMapper[T <: DatabaseInfo] {

  def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: T
  ): MapValue
}

object BaseDatabaseInfoMapper extends DatabaseInfoMapper[DatabaseInfo] {

  override def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: DatabaseInfo
  ): MapValue = {
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
        SERVER_ID_COL
      ),
      Array(
        Values.stringValue(extendedDatabaseInfo.namedDatabaseId().name()),
        Values.stringValue(extendedDatabaseInfo.databaseType()),
        Values.stringValue(extendedDatabaseInfo.access().getStringRepr),
        extendedDatabaseInfo.boltAddress().map[AnyValue](s => Values.stringValue(s.toString)).orElse(Values.NO_VALUE),
        extendedDatabaseInfo.role().map[AnyValue](s => Values.stringValue(s)).orElse(Values.NO_VALUE),
        Values.booleanValue(extendedDatabaseInfo.writer()),
        Values.stringValue(extendedDatabaseInfo.status()),
        Values.stringValue(extendedDatabaseInfo.statusMessage()),
        extendedDatabaseInfo match {
          case info: ExtendedDatabaseInfo => info.externalStoreId().map[AnyValue](s =>
              Values.stringValue(s)
            ).orElse(Values.NO_VALUE)
          case _ => Values.NO_VALUE
        },
        extendedDatabaseInfo.serverId().toScala.map(srvId => Values.stringValue(srvId.uuid().toString)).getOrElse(
          Values.NO_VALUE
        )
      )
    )
  }

}

object CommunityExtendedDatabaseInfoMapper extends DatabaseInfoMapper[ExtendedDatabaseInfo] {

  override def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: ExtendedDatabaseInfo
  ): MapValue =
    BaseDatabaseInfoMapper.toMapValue(databaseManagementService, extendedDatabaseInfo).updatedWith(
      VirtualValues.map(
        Array(
          CURRENT_PRIMARIES_COUNT_COL,
          CURRENT_SECONDARIES_COUNT_COL,
          STORE_COL,
          LAST_COMMITTED_TX_COL,
          REPLICATION_LAG_COL
        ),
        Array(
          Values.longValue(extendedDatabaseInfo.primariesCount()),
          Values.longValue(extendedDatabaseInfo.secondariesCount()),
          extendedDatabaseInfo.storeId().toScala.map(Values.stringValue).getOrElse(Values.NO_VALUE),
          Values.NO_VALUE,
          Values.longValue(0)
        )
      )
    )
}
