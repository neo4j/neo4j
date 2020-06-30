/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.cache

import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker

trait CaffeineCacheFactory {
  def createCache[K <: AnyRef, V <: AnyRef](size: Int): Cache[K, V]
  def createCache[K <: AnyRef, V <: AnyRef](ticker: Ticker, ttl: Long, size: Int): Cache[K, V]
}

class ExecutorBasedCaffeineCacheFactory(executor: Executor) extends CaffeineCacheFactory {
  override def createCache[K <: AnyRef, V <: AnyRef](size: Int): Cache[K, V] = {
    Caffeine
      .newBuilder()
      .executor(executor)
      .maximumSize(size)
      .build[K, V]()
  }

  override def createCache[K <: AnyRef, V <: AnyRef](
      ticker: Ticker,
      ttl: Long,
      size: Int
  ): Cache[K, V] =
    Caffeine
      .newBuilder()
      .executor(executor)
      .maximumSize(size)
      .ticker(ticker)
      .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
      .build[K, V]()
}
