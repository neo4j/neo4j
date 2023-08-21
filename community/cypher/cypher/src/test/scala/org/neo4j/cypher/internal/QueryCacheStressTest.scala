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
package org.neo4j.cypher.internal

import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.verify
import org.neo4j.cypher.internal.QueryCacheTest.TC
import org.neo4j.cypher.internal.QueryCacheTest.compilerWithExpressionCodeGenOption
import org.neo4j.cypher.internal.QueryCacheTest.newCache
import org.neo4j.cypher.internal.QueryCacheTest.newKey
import org.neo4j.cypher.internal.QueryCacheTest.newTracer
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class QueryCacheStressTest extends CypherFunSuite {

  test("should recompile at least once when running from multiple threads") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val futures = Future.sequence((1 to 100).map(_ =>
      Future {
        cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
      }
    ))

    // Then
    Await.ready(futures, 60.seconds)
    verify(tracer, atLeastOnce()).computeWithExpressionCodeGen(key, "")
  }

  test("should hit at least once when running from multiple threads") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val futures = Future.sequence((1 to 100).map(_ =>
      Future {
        cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
      }
    ))

    // Then
    Await.result(futures, 60.seconds)

    verify(tracer, atLeastOnce()).cacheHit(key, "")
    verify(tracer, atLeastOnce()).cacheMiss(key, "")
  }
}
