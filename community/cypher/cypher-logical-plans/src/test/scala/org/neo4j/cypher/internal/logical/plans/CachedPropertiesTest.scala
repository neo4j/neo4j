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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.cachedNodeProp
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.cachedRelProp
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.propName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CachedPropertiesTest extends CypherFunSuite {

  test("CachedProperties.empty should contain an empty map") {
    CachedProperties.empty.entries shouldBe empty
  }

  test("CachedProperties.singleton should contain a singleton map") {
    val cachedProperties = CachedProperties.singleton(v"a", v"b", RELATIONSHIP_TYPE, Set(propName("foo")))
    cachedProperties.entries should contain only (v"a" -> CachedProperties.Entry(
      v"b",
      RELATIONSHIP_TYPE,
      Set(propName("foo"))
    ))
  }

  test("add all: should add disjoint variables to cached properties correctly") {
    // given an empty cache
    val emptyCache = CachedProperties.empty

    // when I add disjoint values
    val cache = emptyCache.addAll(Set(cachedNodeProp("a", "foo", "b"), cachedRelProp("rel", "foo", "rel2")))

    // then it should contain disjoint values
    cache.entries should contain.allOf(
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"))),
      v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))
    )
  }

  test("add all: should collect properties for the same variable") {
    // given an empty cache
    val emptyCache = CachedProperties.empty

    // when I add multiple properties for the same value
    val cache = emptyCache.addAll(Set(cachedNodeProp("a", "foo", "b"), cachedNodeProp("a", "bar", "b")))

    // then it should combine the properties
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar")))
    )
  }

  test(
    "add all: should use the first entity type and original entity name when collecting properties for the same variable"
  ) {
    // given an empty cache
    val emptyCache = CachedProperties.empty

    // when I add multiple properties of different entity types and "original" entity names
    val cache = emptyCache.addAll(Set(cachedNodeProp("a", "foo", "b"), cachedRelProp("rel", "bar", "b")))

    // then it should use the first entity name and type (TODO: should we throw an exception instead?)
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar")))
    )
  }

  test("union: should add disjoint variables to cached properties correctly") {
    // given a cache with node "b" and a cached with node "rel2"
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 =
      CachedProperties(Map(v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))))
    // when I add disjoint values
    val cache = cache1.union(cache2)

    // then it should contain disjoint values
    cache.entries should contain.allOf(
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"))),
      v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))
    )

    // and union should be commutative for disjoint variables
    cache shouldEqual cache2.union(cache1)
  }

  test("union should be idempotent") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.union(cache) shouldEqual cache
  }

  test("union with an empty set should be non-empty") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.union(CachedProperties.empty) shouldEqual cache
  }

  test("union: should collect properties for the same variable") {
    // given two non-empty caches with the same variables
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("bar")))))

    // when I do a union on the two
    val cache = cache1.union(cache2)

    // then it should combine the properties
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar")))
    )

    // should be commutative in this case
    cache2.union(cache1) shouldEqual cache
  }

  test(
    "union: should use the first entity type and original entity name when collecting properties for the same variable"
  ) {
    // given two non-empty caches with the same variables but conflicting definitions
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"b", RELATIONSHIP_TYPE, Set(propName("bar")))))

    // when I do a union on the two
    val cache = cache1.union(cache2)

    // then it should combine the properties
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar")))
    )

    // should not be commutative
    cache2.union(cache1) should not equal cache
  }

  test("intersection: should not add disjoint variables to cached properties") {
    // given a cache with node "b" and a cached with node "rel2"
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 =
      CachedProperties(Map(v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))))
    // when I intersect disjoint values
    val cache = cache1.intersect(cache2)

    // then it should contain disjoint values
    cache.entries shouldBe empty
    // and intersection should be commutative for disjoint variables
    cache shouldEqual cache2.intersect(cache1)
  }

  test("intersection should be idempotent") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.intersect(cache) shouldEqual cache
  }

  test("intersection with an empty set should be empty") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.intersect(CachedProperties.empty) shouldEqual CachedProperties.empty
  }

  test("intersect: should find common properties for the same variable") {
    // given two non-empty caches with the same variables
    val cache1 =
      CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("baz")))))
    val cache2 =
      CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("bar"), propName("baz")))))

    // when I do a intersection on the two
    val cache = cache1.intersect(cache2)

    // then it should filter the common properties
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("baz")))
    )

    // should be commutative in this case
    cache2.intersect(cache1) shouldEqual cache
  }

  test("intersect: should not include variables with no common properties") {
    // given two non-empty caches with the same variables
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("bar")))))

    // when I do an intersection on the two
    val cache = cache1.intersect(cache2)

    // then it should be empty since there are no common properties
    cache.entries shouldBe empty

    // should be commutative in this case
    cache2.intersect(cache1) shouldEqual cache
  }

  test(
    "intersect: should not intersect entries if the original entity is different"
  ) {
    // given two non-empty caches with the same variables but conflicting definitions
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"b", NODE_TYPE, Set(propName("foo")))))

    // when I do an intersection on the two
    val cache = cache1.intersect(cache2)

    // then it should pick the first entity type
    cache.entries shouldBe empty

    // should be commutative
    cache2.intersect(cache1) shouldEqual cache
  }

  test(
    "intersect: should not intersect entries if the entity type is different"
  ) {
    // given two non-empty caches with the same variables but conflicting definitions
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", RELATIONSHIP_TYPE, Set(propName("foo")))))

    // when I do an intersection on the two
    val cache = cache1.intersect(cache2)

    // then it should pick the first entity type
    cache.entries shouldBe empty

    // should be commutative
    cache2.intersect(cache1) shouldEqual cache
  }

  test("properties intersection: should add disjoint variables to cached properties") {
    // given a cache with node "b" and a cached with node "rel2"
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 =
      CachedProperties(Map(v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))))
    // when I intersect properties on disjoint values
    val cache = cache1.intersectProperties(cache2)

    // then it should contain disjoint values
    cache.entries shouldBe Map(
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"))),
      v"rel2" -> CachedProperties.Entry(v"rel", RELATIONSHIP_TYPE, Set(propName("foo")))
    )
    // and intersection should be commutative for disjoint variables
    cache shouldEqual cache2.intersectProperties(cache1)
  }

  test("properties intersection should be idempotent") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.intersectProperties(cache) shouldEqual cache
  }

  test("properties intersection with an empty set should be itself") {
    // Give a non-empty cache
    val cache = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    cache.intersectProperties(CachedProperties.empty) shouldEqual cache
  }

  test("properties intersection: should find common properties for the same variable") {
    // given two non-empty caches with the same variables
    val cache1 =
      CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("baz")))))
    val cache2 =
      CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("bar"), propName("baz")))))

    // when I do a intersection on the two
    val cache = cache1.intersectProperties(cache2)

    // then it should filter the common properties
    cache.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("baz")))
    )

    // should be commutative in this case
    cache2.intersectProperties(cache1) shouldEqual cache
  }

  test("intersect properties: should not include variables with no common properties") {
    // given two non-empty caches with the same variables
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("bar")))))

    // when I do an intersection on the two
    val cache = cache1.intersectProperties(cache2)

    // then it should be empty since there are no common properties
    cache.entries shouldBe empty

    // should be commutative in this case
    cache2.intersectProperties(cache1) shouldEqual cache
  }

  test(
    "intersect properties: should not intersect entries if the original entity is different"
  ) {
    // given two non-empty caches with the same variables but conflicting definitions
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"b", NODE_TYPE, Set(propName("foo")))))

    // when I do an intersection on the two
    val cache = cache1.intersectProperties(cache2)

    // then it should pick the first entity type
    cache.entries shouldBe empty

    // should be commutative
    cache2.intersectProperties(cache1) shouldEqual cache
  }

  test(
    "intersect properties: should not intersect entries if the entity type is different"
  ) {
    // given two non-empty caches with the same variables but conflicting definitions
    val cache1 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))
    val cache2 = CachedProperties(Map(v"b" -> CachedProperties.Entry(v"a", RELATIONSHIP_TYPE, Set(propName("foo")))))

    // when I do an intersection on the two
    val cache = cache1.intersectProperties(cache2)

    // then it should pick the first entity type
    cache.entries shouldBe empty

    // should be commutative
    cache2.intersectProperties(cache1) shouldEqual cache
  }

  test("rename: should clone existing properties correctly") {
    // given a non-empty caches with the variable a
    val cache = CachedProperties(Map(v"a" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    // when I rename "a" to "b"
    val cacheAfterRename = cache.rename(Map(v"a" -> v"b"))

    // then it should have copied the data to the entry "b" correctly
    cacheAfterRename.entries should contain only (
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))
    )
  }

  test("rename: should keep entries that have not been renamed") {
    // given a non-empty caches with the variable a
    val cache = CachedProperties(Map(v"a" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))))

    // when I rename "c" to "b"
    val cacheAfterRename = cache.rename(Map(v"c" -> v"b"))

    // then it should have copied the data to the entry "b" correctly
    cacheAfterRename shouldEqual cache
  }

  test("rename: should remove conflicting entries") {
    // given a non-empty caches with the variable a and b
    val cache = CachedProperties(Map(
      v"a" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"))),
      v"b" -> CachedProperties.Entry(v"b", NODE_TYPE, Set(propName("bar")))
    ))

    // when I rename "a" to "b"
    val cacheAfterRename = cache.rename(Map(v"a" -> v"b"))

    // then it should have copied the data from "a" to "b" correctly and removed "b"
    cacheAfterRename.entries should contain(
      v"b" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo")))
    )
  }

  test("contain: should identify presence by variable and property") {
    // given a non-empty caches with the variable a
    val cache = CachedProperties(Map(
      v"a" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar")))
    ))

    // then contains should identify by both property
    cache.contains(v"a", propName("foo")) shouldBe true
    cache.contains(v"a", propName("unknown")) shouldBe false
    cache.contains(v"unknown", propName("foo")) shouldBe false
  }

  test("retain: should only retain properties and variables in the map") {
    // given a non-empty caches with the variable a and b
    val cache = CachedProperties(Map(
      v"a" -> CachedProperties.Entry(v"a", NODE_TYPE, Set(propName("foo"), propName("bar"))),
      v"b" -> CachedProperties.Entry(v"b", NODE_TYPE, Set(propName("foo"), propName("bar")))
    ))

    // then retain a->foo should only keep property foo for a
    cache.retain(Map(v"a" -> Set(propName("foo")))).entries shouldEqual Map(v"a" -> CachedProperties.Entry(
      v"a",
      NODE_TYPE,
      Set(propName("foo"))
    ))
  }
}
