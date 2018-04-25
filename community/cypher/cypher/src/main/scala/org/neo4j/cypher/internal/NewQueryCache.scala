/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.neo4j.kernel.impl.query.TransactionalContext

/**
  * The result of one cache lookup.
  */
sealed trait CacheLookup
case class CacheHit(executableQuery: CachedExecutableQuery) extends CacheLookup
case class CacheMiss(executableQuery: CachedExecutableQuery) extends CacheLookup

/**
  * Tracer for cache activity.
  */
trait CacheTracer {
  def queryCacheHit(queryKey: String): Unit

  def queryCacheMiss(queryKey: String): Unit

  def queryCacheStale(queryKey: String, secondsSincePlan: Int): Unit

  def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit
}

/**
  * Cache which maps query strings into CachedExecutableQueries.
  *
  * This cache knows that CachedExecutableQueries can become stale, and uses a
  * PlanStalenessCaller to verify that CEQs are reusable before returning. A CEQ
  * which is detected in the cache, but is found to be stale
  *
  * @param size
  * @param stalenessCaller
  * @param tracer
  */
class NewQueryCache(val size: Int,
                    val stalenessCaller: PlanStalenessCaller,
                    val tracer: CacheTracer) {

  val inner: Cache[String, CachedExecutableQuery] = Caffeine.newBuilder().maximumSize(size).build[String, CachedExecutableQuery]()

  /**
    * TODO
    *
    * @param queryKey
    * @param tc
    * @param compiler
    * @return
    */
  def computeIfAbsentOrStale(queryKey: String,
                             tc: TransactionalContext,
                             compiler: TransactionalContext => CachedExecutableQuery
                            ): CacheLookup = {
    if (size == 0)
      CacheMiss(compiler(tc))
    else {
      val executableQuery = inner.getIfPresent(queryKey)
      if (executableQuery != null) {
        stalenessCaller.staleness(tc, executableQuery) match {
          case NotStale =>
            tracer.queryCacheHit(queryKey)
            CacheHit(executableQuery)
          case Stale(secondsSincePlan) =>
            tracer.queryCacheStale(queryKey, secondsSincePlan)
            recompile(queryKey, tc, compiler)
        }
      } else
        recompile(queryKey, tc, compiler)
    }
  }

  private def recompile(queryKey: String,
                        tc: TransactionalContext,
                        compiler: TransactionalContext => CachedExecutableQuery
                       ): CacheLookup = {
    var recompiled = false
    val executableQuery =
      inner.get(queryKey, new java.util.function.Function[String, CachedExecutableQuery] {
        def apply(queryKey: String): CachedExecutableQuery = {
          recompiled = true
          compiler(tc)
        }
      })

    if (recompiled) {
      tracer.queryCacheMiss(queryKey)
      CacheMiss(executableQuery)
    } else {
      tracer.queryCacheHit(queryKey)
      CacheHit(executableQuery)
    }
  }

  /**
    * Method for clearing the LRUCache
    *
    * @return the number of elements in the cache prior to the clearing
    */
  def clear(): Long = {
    val priorSize = inner.estimatedSize()
    inner.invalidateAll()
    inner.cleanUp()
    tracer.queryCacheFlush(priorSize)
    priorSize
  }
}
