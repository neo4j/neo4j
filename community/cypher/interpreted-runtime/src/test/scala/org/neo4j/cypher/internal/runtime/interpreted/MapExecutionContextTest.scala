/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.values.storable.BooleanValue
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class MapExecutionContextTest extends CypherFunSuite {
  test("create clone") {
    // given
    val key = "key"
    val ctx = ExecutionContext.empty.copyWith(key, BooleanValue.FALSE)

    // when
    val ctxClone = ctx.createClone()

    // then
    ctxClone.contains(key) should equal(true)
    ctx(key) should equal(BooleanValue.FALSE)
    ctxClone should not be theSameInstanceAs(ctx)

    mutatingLeftDoesNotAffectRight(ctx, ctxClone)
  }

  test("single key set") {
    // given
    val key = "key"
    val ctx = ExecutionContext.empty

    // when (written)
    ctx.set(key, BooleanValue.FALSE)

    // then
    ctx.contains(key) should equal(true)
    ctx(key) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key, BooleanValue.TRUE)

    // then
    ctx.contains(key) should equal(true)
    ctx(key) should equal(BooleanValue.TRUE)
  }

  test("double key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val ctx = ExecutionContext.empty

    // when (written)
    ctx.set(key1, BooleanValue.FALSE, key2, BooleanValue.FALSE)

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx(key1) should equal(BooleanValue.FALSE)
    ctx(key2) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx(key1) should equal(BooleanValue.TRUE)
    ctx(key2) should equal(BooleanValue.TRUE)
  }

  test("triple key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"
    val ctx = ExecutionContext.empty

    // when (written)
    ctx.set(key1, BooleanValue.FALSE, key2, BooleanValue.FALSE, key3, BooleanValue.FALSE)

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx.contains(key3) should equal(true)
    ctx(key1) should equal(BooleanValue.FALSE)
    ctx(key2) should equal(BooleanValue.FALSE)
    ctx(key3) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE, key3, BooleanValue.TRUE)

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx.contains(key3) should equal(true)
    ctx(key1) should equal(BooleanValue.TRUE)
    ctx(key2) should equal(BooleanValue.TRUE)
    ctx(key3) should equal(BooleanValue.TRUE)
  }

  test("many key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val ctx = ExecutionContext.empty

    // when (written)
    ctx.set(Seq((key1, BooleanValue.FALSE), (key2, BooleanValue.FALSE)))

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx(key1) should equal(BooleanValue.FALSE)
    ctx(key2) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(Seq((key1, BooleanValue.TRUE), (key2, BooleanValue.TRUE)))

    // then
    ctx.contains(key1) should equal(true)
    ctx.contains(key2) should equal(true)
    ctx(key1) should equal(BooleanValue.TRUE)
    ctx(key2) should equal(BooleanValue.TRUE)
  }

  test("mergeWith - no cached properties") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = ExecutionContext.empty.copyWith(key1, BooleanValue.FALSE)

    // when (other map is equal or larger)
    val rhsCtx1 = ExecutionContext.empty.copyWith(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    lhsCtx.mergeWith(rhsCtx1)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = ExecutionContext.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = prop("n", "key")
    val lhsCtx = ExecutionContext.empty.copyWith(key1, BooleanValue.FALSE)
    val rhsCtx = ExecutionContext.empty
    rhsCtx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    rhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    lhsCtx.mergeWith(ExecutionContext.empty.copyWith(key2, BooleanValue.FALSE))

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx, lhsCtx)
  }

  test("mergeWith - cached properties on lhs only") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = prop("n", "key")
    val lhsCtx = ExecutionContext.empty.copyWith(key1, BooleanValue.FALSE)
    lhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    val rhsCtx1 = ExecutionContext.empty
    rhsCtx1.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx1)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = ExecutionContext.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("mergeWith - cached properties on both sides") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = prop("n", "key")
    val lhsCtx = ExecutionContext.empty.copyWith(key1, BooleanValue.FALSE)
    lhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    val rhsCtx1 = ExecutionContext.empty
    rhsCtx1.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    rhsCtx1.setCachedProperty(cachedPropertyKey, BooleanValue.FALSE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx1)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.FALSE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = ExecutionContext.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2)

    // then
    lhsCtx.contains(key1) should equal(true)
    lhsCtx.contains(key2) should equal(true)
    lhsCtx(key1) should equal(BooleanValue.TRUE)
    lhsCtx(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("set/get cached property") {
    // given
    val key = prop("n", "key")
    val ctx = ExecutionContext.empty

    // when (written)
    ctx.setCachedProperty(key, BooleanValue.FALSE)

    // then
    ctx.getCachedProperty(key) should equal(BooleanValue.FALSE)

    // when (overwritten)
    ctx.setCachedProperty(key, BooleanValue.TRUE)

    // then
    ctx.getCachedProperty(key) should equal(BooleanValue.TRUE)
  }

  test("single key copy") {
    // given
    val key = "key"
    val lhsCtx = ExecutionContext.empty

    // when
    val rhsCtx = lhsCtx.copyWith(key, BooleanValue.FALSE)

    // then
    lhsCtx.contains(key) should equal(false)
    rhsCtx.contains(key) should equal(true)
    rhsCtx(key) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(lhsCtx, rhsCtx)
  }

  test("double key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = ExecutionContext.empty

    // when
    val rhsCtx = lhsCtx.copyWith(key1, BooleanValue.FALSE, key2, BooleanValue.TRUE)

    // then
    lhsCtx.contains(key1) should equal(false)
    lhsCtx.contains(key2) should equal(false)
    rhsCtx.contains(key1) should equal(true)
    rhsCtx.contains(key2) should equal(true)
    rhsCtx(key1) should equal(BooleanValue.FALSE)
    rhsCtx(key2) should equal(BooleanValue.TRUE)

    mutatingLeftDoesNotAffectRight(lhsCtx, rhsCtx)
  }

  test("triple key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"
    val lhsCtx = ExecutionContext.empty

    // when
    val newCtx = lhsCtx.copyWith(key1, BooleanValue.FALSE, key2, BooleanValue.TRUE, key3, BooleanValue.TRUE)

    // then
    lhsCtx.contains(key1) should equal(false)
    lhsCtx.contains(key2) should equal(false)
    lhsCtx.contains(key3) should equal(false)
    newCtx.contains(key1) should equal(true)
    newCtx.contains(key2) should equal(true)
    newCtx.contains(key3) should equal(true)
    newCtx(key1) should equal(BooleanValue.FALSE)
    newCtx(key2) should equal(BooleanValue.TRUE)
    newCtx(key3) should equal(BooleanValue.TRUE)

    mutatingLeftDoesNotAffectRight(lhsCtx, newCtx)
  }

  test("many key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = ExecutionContext.empty

    // when (written)
    val newCtx = lhsCtx.copyWith(Seq((key1, BooleanValue.FALSE), (key2, BooleanValue.FALSE)))

    // then
    lhsCtx.contains(key1) should equal(false)
    lhsCtx.contains(key2) should equal(false)
    newCtx.contains(key1) should equal(true)
    newCtx.contains(key2) should equal(true)
    newCtx(key1) should equal(BooleanValue.FALSE)
    newCtx(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(lhsCtx, newCtx)
  }

  private def mutatingLeftDoesNotAffectRight(left: ExecutionContext, right: ExecutionContext): Unit = {
    // given
    left should not be theSameInstanceAs(right)
    val newKey = "this key should not yet exist in left or right"
    val newCachedPropertyKey = prop("n", newKey)
    left.contains(newKey) should equal(false)
    right.contains(newKey) should equal(false)
    an[NoSuchElementException] should be thrownBy left.getCachedProperty(newCachedPropertyKey)
    an[NoSuchElementException] should be thrownBy right.getCachedProperty(newCachedPropertyKey)

    // when (left is modified)
    left.set(newKey, BooleanValue.TRUE)
    left.setCachedProperty(newCachedPropertyKey, BooleanValue.FALSE)

    // then (only left should be modified)
    left(newKey) should equal(BooleanValue.TRUE)
    left.getCachedProperty(newCachedPropertyKey) should equal(BooleanValue.FALSE)
    right.contains(newKey) should equal(false)
    an[NoSuchElementException] should be thrownBy right.getCachedProperty(newCachedPropertyKey)
  }

  private def prop(node: String, prop: String) =
    CachedNodeProperty(node, PropertyKeyName(prop)(InputPosition.NONE))(InputPosition.NONE)
}
