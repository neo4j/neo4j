/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * The result of one cache lookup.
  */
sealed trait CacheLookup[EXECUTABLE_QUERY] {
  def executableQuery: EXECUTABLE_QUERY
}
case class CacheHit[EXECUTABLE_QUERY](executableQuery: EXECUTABLE_QUERY) extends CacheLookup[EXECUTABLE_QUERY]
case class CacheMiss[EXECUTABLE_QUERY](executableQuery: EXECUTABLE_QUERY) extends CacheLookup[EXECUTABLE_QUERY]
case class CacheDisabled[EXECUTABLE_QUERY](executableQuery: EXECUTABLE_QUERY) extends CacheLookup[EXECUTABLE_QUERY]

/**
  * Tracer for cache activity.
  */
trait CacheTracer[QUERY_KEY] {
  def queryCacheHit(queryKey: QUERY_KEY, metaData: String): Unit

  def queryCacheMiss(queryKey: QUERY_KEY, metaData: String): Unit

  def queryCacheStale(queryKey: QUERY_KEY, secondsSincePlan: Int, metaData: String): Unit

  def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit
}

/**
  * Cache which maps query strings into CachedExecutableQueries.
  *
  * This cache knows that CachedExecutableQueries can become stale, and uses a
  * PlanStalenessCaller to verify that CEQs are reusable before returning. A CEQ
  * which is detected in the cache, but is found to be stale
  *
  * @param maximumSize Maximum size of this cache
  * @param stalenessCaller Decided whether CachedExecutionPlans are stale
  * @param tracer Traces cache activity
  */
class QueryCache[QUERY_REP <: AnyRef, QUERY_KEY <: Pair[QUERY_REP, MapValue], EXECUTABLE_QUERY <: AnyRef](val maximumSize: Int,
                                                                                                          val stalenessCaller: PlanStalenessCaller[EXECUTABLE_QUERY],
                                                                                                          val tracer: CacheTracer[Pair[QUERY_REP, ParameterTypeMap]]) {
  type Cache_Key = Pair[QUERY_REP, ParameterTypeMap]

  val inner: Cache[Cache_Key, EXECUTABLE_QUERY] = Caffeine.newBuilder().maximumSize(maximumSize).build[Cache_Key, EXECUTABLE_QUERY]()

  import QueryCache.NOT_PRESENT


  private def extractParameterTypeMap(value: MapValue) = {
    val resultMap = new ParameterTypeMap()
    for( key <- value.keySet().iterator()) {
      resultMap.put(key, value.get(key).getClass)
    }
    resultMap
  }

  /**
    * Retrieve the CachedExecutionPlan associated with the given queryKey, or compile, cache and
    * return the query if it is not in the cache, or the cached execution plan is stale.
    *
    * @param queryKey the queryKey to retrieve the execution plan for
    * @param tc TransactionalContext in which to compile and compute staleness
    * @param compile Compiler to use if the query is not cached or stale
    * @param metaData String which will be passed to the CacheTracer
    * @return A CacheLookup with an CachedExecutionPlan
    */
  def computeIfAbsentOrStale(queryKey: QUERY_KEY,
                             tc: TransactionalContext,
                             compile: () => EXECUTABLE_QUERY,
                             metaData: String = ""
                            ): CacheLookup[EXECUTABLE_QUERY] = {
    if (maximumSize == 0)
      CacheDisabled(compile())
    else {
      val actualKey = Pair.of(queryKey.first(), extractParameterTypeMap(queryKey.other()))
      inner.getIfPresent(actualKey) match {
        case NOT_PRESENT =>
          compileAndCache(actualKey, tc, compile, metaData)

        case executableQuery =>
          stalenessCaller.staleness(tc, executableQuery) match {
            case NotStale =>
              hit(actualKey, executableQuery, metaData)
            case Stale(secondsSincePlan) =>
              tracer.queryCacheStale(actualKey, secondsSincePlan, metaData)
              compileAndCache(actualKey, tc, compile, metaData)
          }
      }
    }
  }


  /**
    * Ensure this query is recompiled and put it in the cache.
    *
    * Compilation is either done in this thread, or by some other thread if it got there
    * first. Regardless of who does it, this is treated as a cache miss, because it will
    * take a long time.
    */
  private def compileAndCache(key: Cache_Key,
                        tc: TransactionalContext,
                        compile: () => EXECUTABLE_QUERY,
                        metaData: String
                       ): CacheLookup[EXECUTABLE_QUERY] = {
    val newExecutableQuery = compile()
    inner.put(key, newExecutableQuery)
    miss(key, newExecutableQuery, metaData)
  }

  private def hit(key: Cache_Key,
                  executableQuery: EXECUTABLE_QUERY,
                  metaData: String) = {
    tracer.queryCacheHit(key, metaData)
    CacheHit(executableQuery)
  }

  private def miss(key: Cache_Key,
                   newExecutableQuery: EXECUTABLE_QUERY,
                   metaData: String) = {
    tracer.queryCacheMiss(key, metaData)
    CacheMiss(newExecutableQuery)
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

object QueryCache {
  val NOT_PRESENT: ExecutableQuery = null
}

/*
   The whole point of this class is to have a map that in addition to key equality, also checks its values for type equality
 */
class ParameterTypeMap extends mutable.HashMap[String, Class[_]] {
  override def equals(that: Any): Boolean = {
    if (!that.isInstanceOf[ParameterTypeMap])
      return false

    val other = that.asInstanceOf[ParameterTypeMap]

    if ( this.size != other.size )
      return false

    for ( key <- this.keys )
    {
      val oneType = this (key)
      val otherType = other(key)
      if (!(oneType == otherType)) {
        return false
      }
    }
    true
  }
}
