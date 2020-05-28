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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class MapCypherRowTest extends CypherFunSuite with AstConstructionTestSupport {
  test("create clone") {
    // given
    val key = "key"
    val ctx = CypherRow.empty.copyWith(key, BooleanValue.FALSE)

    // when
    val ctxClone = ctx.createClone()

    // then
    ctx.getByName(key) should equal(BooleanValue.FALSE)
    ctxClone should not be theSameInstanceAs(ctx)

    mutatingLeftDoesNotAffectRight(ctx, ctxClone)
  }

  test("single key set") {
    // given
    val key = "key"
    val ctx = CypherRow.empty

    // when (written)
    ctx.set(key, BooleanValue.FALSE)

    // then
    ctx.getByName(key) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key, BooleanValue.TRUE)

    // then
    ctx.getByName(key) should equal(BooleanValue.TRUE)
  }

  test("double key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val ctx = CypherRow.empty

    // when (written)
    ctx.set(key1, BooleanValue.FALSE, key2, BooleanValue.FALSE)

    // then
    ctx.getByName(key1) should equal(BooleanValue.FALSE)
    ctx.getByName(key2) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)

    // then
    ctx.getByName(key1) should equal(BooleanValue.TRUE)
    ctx.getByName(key2) should equal(BooleanValue.TRUE)
  }

  test("triple key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"
    val ctx = CypherRow.empty

    // when (written)
    ctx.set(key1, BooleanValue.FALSE, key2, BooleanValue.FALSE, key3, BooleanValue.FALSE)

    // then
    ctx.getByName(key1) should equal(BooleanValue.FALSE)
    ctx.getByName(key2) should equal(BooleanValue.FALSE)
    ctx.getByName(key3) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE, key3, BooleanValue.TRUE)

    // then
    ctx.getByName(key1) should equal(BooleanValue.TRUE)
    ctx.getByName(key2) should equal(BooleanValue.TRUE)
    ctx.getByName(key3) should equal(BooleanValue.TRUE)
  }

  test("many key set") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val ctx = CypherRow.empty

    // when (written)
    ctx.set(Seq((key1, BooleanValue.FALSE), (key2, BooleanValue.FALSE)))

    // then
    ctx.getByName(key1) should equal(BooleanValue.FALSE)
    ctx.getByName(key2) should equal(BooleanValue.FALSE)

    // when (over written)
    ctx.set(Seq((key1, BooleanValue.TRUE), (key2, BooleanValue.TRUE)))

    // then
    ctx.getByName(key1) should equal(BooleanValue.TRUE)
    ctx.getByName(key2) should equal(BooleanValue.TRUE)
  }

  test("mergeWith - no cached properties") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = CypherRow.empty.copyWith(key1, BooleanValue.FALSE)

    // when (other map is equal or larger)
    val rhsCtx1 = CypherRow.empty.copyWith(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    lhsCtx.mergeWith(rhsCtx1, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = CypherRow.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("mergeWith - cached properties on rhs only") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = cachedNodeProp("n", "key")
    val lhsCtx = CypherRow.empty.copyWith(key1, BooleanValue.FALSE)
    val rhsCtx = CypherRow.empty
    rhsCtx.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    rhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    lhsCtx.mergeWith(CypherRow.empty.copyWith(key2, BooleanValue.FALSE), null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx, lhsCtx)
  }

  test("mergeWith - cached properties on lhs only") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = cachedNodeProp("n", "key")
    val lhsCtx = CypherRow.empty.copyWith(key1, BooleanValue.FALSE)
    lhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    val rhsCtx1 = CypherRow.empty
    rhsCtx1.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx1, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.TRUE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = CypherRow.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("mergeWith - cached properties on both sides") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val cachedPropertyKey = cachedNodeProp("n", "key")
    val lhsCtx = CypherRow.empty.copyWith(key1, BooleanValue.FALSE)
    lhsCtx.setCachedProperty(cachedPropertyKey, BooleanValue.TRUE)

    val rhsCtx1 = CypherRow.empty
    rhsCtx1.set(key1, BooleanValue.TRUE, key2, BooleanValue.TRUE)
    rhsCtx1.setCachedProperty(cachedPropertyKey, BooleanValue.FALSE)

    // when (other map is equal or larger)
    lhsCtx.mergeWith(rhsCtx1, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.TRUE)
    lhsCtx.getCachedProperty(cachedPropertyKey) should equal(BooleanValue.FALSE)

    // when (other map is smaller, the missing keys should not be removed)
    val rhsCtx2 = CypherRow.empty.copyWith(key2, BooleanValue.FALSE)
    lhsCtx.mergeWith(rhsCtx2, null)

    // then
    lhsCtx.getByName(key1) should equal(BooleanValue.TRUE)
    lhsCtx.getByName(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(rhsCtx1, lhsCtx)
    mutatingLeftDoesNotAffectRight(rhsCtx2, lhsCtx)
  }

  test("set/get cached property") {
    // given
    val key = cachedNodeProp("n", "key")
    val ctx = CypherRow.empty

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
    val lhsCtx = CypherRow.empty

    // when
    val rhsCtx = lhsCtx.copyWith(key, BooleanValue.FALSE)

    // then
    rhsCtx.getByName(key) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(lhsCtx, rhsCtx)
  }

  test("double key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = CypherRow.empty

    // when
    val rhsCtx = lhsCtx.copyWith(key1, BooleanValue.FALSE, key2, BooleanValue.TRUE)

    // then
    rhsCtx.getByName(key1) should equal(BooleanValue.FALSE)
    rhsCtx.getByName(key2) should equal(BooleanValue.TRUE)

    mutatingLeftDoesNotAffectRight(lhsCtx, rhsCtx)
  }

  test("triple key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"
    val lhsCtx = CypherRow.empty

    // when
    val newCtx = lhsCtx.copyWith(key1, BooleanValue.FALSE, key2, BooleanValue.TRUE, key3, BooleanValue.TRUE)

    // then
    newCtx.getByName(key1) should equal(BooleanValue.FALSE)
    newCtx.getByName(key2) should equal(BooleanValue.TRUE)
    newCtx.getByName(key3) should equal(BooleanValue.TRUE)

    mutatingLeftDoesNotAffectRight(lhsCtx, newCtx)
  }

  test("many key copy") {
    // given
    val key1 = "key1"
    val key2 = "key2"
    val lhsCtx = CypherRow.empty

    // when (written)
    val newCtx = lhsCtx.copyWith(Seq((key1, BooleanValue.FALSE), (key2, BooleanValue.FALSE)))

    // then
    newCtx.getByName(key1) should equal(BooleanValue.FALSE)
    newCtx.getByName(key2) should equal(BooleanValue.FALSE)

    mutatingLeftDoesNotAffectRight(lhsCtx, newCtx)
  }

  test("should not consider nulled cached properties in estimatedHeapUsage") {
    val row = CypherRow.empty
    val node = VirtualValues.node(42)
    row.set("x", node)
    row.setCachedProperty( cachedNodeProp("x", "prop"), Values.stringValue("foo"))
    row.invalidateCachedNodeProperties(42)

    row.estimatedHeapUsage should be >= node.estimatedHeapUsage()
  }

  private def mutatingLeftDoesNotAffectRight(left: CypherRow, right: CypherRow): Unit = {
    // given
    left should not be theSameInstanceAs(right)
    val newKey = "this key should not yet exist in left or right"
    val newCachedPropertyKey = cachedNodeProp("n", newKey)
    left.getCachedProperty(newCachedPropertyKey) shouldBe null
    right.getCachedProperty(newCachedPropertyKey) shouldBe null

    // when (left is modified)
    left.set(newKey, BooleanValue.TRUE)
    left.setCachedProperty(newCachedPropertyKey, BooleanValue.FALSE)

    // then (only left should be modified)
    left.getByName(newKey) should equal(BooleanValue.TRUE)
    left.getCachedProperty(newCachedPropertyKey) should equal(BooleanValue.FALSE)
    right.getCachedProperty(newCachedPropertyKey) shouldBe null
  }
}
