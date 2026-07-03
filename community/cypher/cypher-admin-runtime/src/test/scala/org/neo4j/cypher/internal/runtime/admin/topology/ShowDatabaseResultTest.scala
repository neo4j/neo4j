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

import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.DatabaseDetails.TYPE_COMPOSITE
import org.neo4j.dbms.database.DatabaseDetails.TYPE_PROPERTY_SHARD
import org.neo4j.dbms.database.DatabaseDetails.TYPE_STANDARD
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseStatus.ONLINE
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.storageengine.api.ExternalStoreId
import org.neo4j.storageengine.api.StoreId
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.time.ZonedDateTime
import java.util.Collections
import java.util.Optional
import java.util.OptionalLong
import java.util.UUID

class ShowDatabaseResultTest extends CypherFunSuite {

  test("should create ShowDatabaseResult for regular db") {
    val details = makeDatabaseDetails(TYPE_STANDARD)

    val result = ShowDatabaseResult(
      details = details,
      default = false,
      home = true,
      constituents = Seq("mydb"),
      aliases = Seq("alias1", "alias2"),
      graphShards = Some(Seq("shard1", "shard2"))
    )

    assert(result.roleValue().asInstanceOf[StringValue].stringValue() == "role")
    assert(result.requestedPrimariesCountValue() == Values.longValue(3L))
    assert(result.requestedSecondariesCountValue() == Values.longValue(1L))
    assert(result.requestedPropertyShardsReplicaCountValue() == Values.NO_VALUE)
    assert(result.currentPrimariesCountValue() == Values.longValue(3L))
    assert(result.currentSecondariesCountValue() == Values.longValue(1L))
    assert(result.currentPropertyShardsReplicaCountValue() == Values.NO_VALUE)
    assert(result.storeValue() == Values.stringValue("store-fmt-1.1"))
    assert(result.optionsValue() == VirtualValues.EMPTY_MAP)
  }

  test("should create ShowDatabaseResult for composite db") {
    val details = makeDatabaseDetails(TYPE_COMPOSITE)

    val result = ShowDatabaseResult(
      details = details,
      default = false,
      home = true,
      constituents = Seq("mydb"),
      aliases = Seq("alias1", "alias2"),
      graphShards = Some(Seq("shard1", "shard2"))
    )

    assert(result.roleValue().asInstanceOf[StringValue].stringValue() == "role")
    assert(result.requestedPrimariesCountValue() == Values.longValue(3L))
    assert(result.requestedSecondariesCountValue() == Values.longValue(1L))
    assert(result.requestedPropertyShardsReplicaCountValue() == Values.NO_VALUE)
    assert(result.currentPrimariesCountValue() == Values.NO_VALUE)
    assert(result.currentSecondariesCountValue() == Values.NO_VALUE)
    assert(result.currentPropertyShardsReplicaCountValue() == Values.NO_VALUE)
    assert(result.storeValue() == Values.NO_VALUE)
    assert(result.optionsValue() == Values.NO_VALUE)
  }

  test("should create ShowDatabaseResult for property shard") {
    val details = makeDatabaseDetails(TYPE_PROPERTY_SHARD)

    val result = ShowDatabaseResult(
      details = details,
      default = false,
      home = true,
      constituents = Seq("mydb"),
      aliases = Seq("alias1", "alias2"),
      graphShards = Some(Seq("shard1", "shard2"))
    )

    assert(result.roleValue().asInstanceOf[StringValue].stringValue() == "property shard replica")
    assert(result.requestedPrimariesCountValue() == Values.NO_VALUE)
    assert(result.requestedSecondariesCountValue() == Values.NO_VALUE)
    assert(result.requestedPropertyShardsReplicaCountValue() == Values.longValue(1L))
    assert(result.currentPrimariesCountValue() == Values.NO_VALUE)
    assert(result.currentSecondariesCountValue() == Values.NO_VALUE)
    assert(result.currentPropertyShardsReplicaCountValue() == Values.longValue(1L))
    assert(result.storeValue() == Values.stringValue("store-fmt-1.1"))
    assert(result.optionsValue() == VirtualValues.EMPTY_MAP)
  }

  private def makeDatabaseDetails(dbType: String): DatabaseDetails = {
    new DatabaseDetails(
      Optional.empty(), // serverId
      READ_WRITE,
      Optional.of(new SocketAddress("boltAddress", 7687)),
      Optional.of("role"),
      true, // writer
      "status",
      "statusMsg",
      OptionalLong.empty,
      OptionalLong.of(0L),
      OptionalLong.empty,
      DatabaseIdFactory.from("neo4j", UUID.randomUUID()),
      ONLINE.statusName,
      dbType,
      Collections.emptyMap,
      Optional.of(StoreId.generateNew("store", "fmt", 1, 1)),
      Optional.of(new ExternalStoreId(UUID.randomUUID())),
      3,
      3,
      1,
      1,
      Optional.of(ZonedDateTime.now()),
      Optional.of(ZonedDateTime.now()),
      Optional.of(ZonedDateTime.now()),
      Optional.of(CypherVersion.Cypher25)
    )
  }
}
