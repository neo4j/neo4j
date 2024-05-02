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
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.GraphDatabaseInternalSettings

/**
 * Flag differentiating between property caching in a standard database, and retrieving properties from shards in a
 * sharded properties database.
 */
sealed trait PropertyCachingMode {
  def isRemoteBatchProperties: Boolean
}

object PropertyCachingMode {

  /**
   * Standard property caching logic to reduce reads from disk.
   */
  case object CacheProperties extends PropertyCachingMode {
    override def isRemoteBatchProperties: Boolean = false
  }

  /**
   * Used only in sharded properties databases to reduce the number of queries issued to shards.
   */
  case object RemoteBatchProperties extends PropertyCachingMode {
    override def isRemoteBatchProperties: Boolean = true
  }

  def fromSetting(setting: GraphDatabaseInternalSettings.PropertyCachingMode): PropertyCachingMode =
    setting match {
      case GraphDatabaseInternalSettings.PropertyCachingMode.CACHE_PROPERTIES        => CacheProperties
      case GraphDatabaseInternalSettings.PropertyCachingMode.REMOTE_BATCH_PROPERTIES => RemoteBatchProperties
    }
}
