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
package org.neo4j.cypher.internal.cache

import com.github.benmanes.caffeine.cache.Cache
import org.neo4j.function.Observable

sealed trait CacheSize {

  def currentValue: Int

  def withSize[K, V, C <: Cache[K, V]](createCacheWithSize: Long => C): C = {
    this match {
      case CacheSize.Static(size) =>
        createCacheWithSize(size)
      case CacheSize.Dynamic(sizeStream) =>
        val cache = createCacheWithSize(sizeStream.latestValue().toInt)
        sizeStream.subscribe(newSize => cache.policy().eviction().ifPresent(_.setMaximum(newSize.toInt)))
        cache
    }
  }
}

object CacheSize {

  case class Static(size: Int) extends CacheSize {
    override def currentValue: Int = size
  }

  case class Dynamic(sizeStream: Observable[Integer]) extends CacheSize {
    override def currentValue: Int = sizeStream.latestValue()
  }
}
