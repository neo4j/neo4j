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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.BitSet

class DefaultIdRegistryTest extends CypherFunSuite {

  test("register and lookup") {
    val r = new DefaultIdRegistry[String]
    val id = r.register("a")
    val id2 = r.register("b")
    id should not be 0 // 0 is reserved for "sorted"
    id2 should not be id
    r.lookup(id) should equal(Some("a"))
    r.lookup(id2) should equal(Some("b"))
  }

  test("Registering same element yields same id") {
    val r = new DefaultIdRegistry[String]
    val id = r.register("a")
    r.register("a") should equal(id)
  }

  test("registerAll and lookup") {
    val r = new DefaultIdRegistry[String]
    val strings = Set("a", "b", "c")
    val ids = r.registerAll(strings)
    ids should not contain 0 // 0 is reserved for "sorted"
    ids.flatMap(r.lookup) should equal(strings)
  }

  test("registerAll multiple times") {
    val r = new DefaultIdRegistry[String]
    val strings = Set("a", "b", "c")
    val ids = r.registerAll(strings)
    ids should not contain 0 // 0 is reserved for "sorted"
    ids.flatMap(r.lookup) should equal(strings)
    val moreStrings = Set("d", "e")
    val moreIds = r.registerAll(moreStrings)
    ids & moreIds should be(empty)
    moreIds.flatMap(r.lookup) should equal(moreStrings)
  }

  test("single compaction") {
    val r = new DefaultIdRegistry[String]
    val strings = Set("a", "b", "c", "d", "e")
    val ids = r.registerAll(strings)
    val subsetToCompact = ids.subsets(2).next()

    r.compacted() should be(false)

    val newId = r.compact(subsetToCompact)

    // Has to be a new ID
    ids should not contain newId
    r.compacted() should be(true)

    // lookup does not use compaction information
    r.lookup(newId) should be(empty)

    // Explode gives back the correct strings
    val originalIDs = (ids -- subsetToCompact).subsets(2).next()
    val idsToExplode = originalIDs + newId
    r.explode(idsToExplode) should equal(
      subsetToCompact.flatMap(r.lookup) ++ originalIDs.flatMap(r.lookup)
    )
    r.explodedBitSet(idsToExplode) should equal(subsetToCompact ++ originalIDs)
  }

  test("multiple compactions") {
    val r = new DefaultIdRegistry[String]
    val strings = Set("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    val ids = r.registerAll(strings)
    val compact1 = ids.subsets(2).next()
    val compact2 = (ids -- compact1).subsets(2).next()
    val newId1 = r.compact(compact1)
    val newId2 = r.compact(compact2)

    newId1 should not be newId2

    // Compact the previously compacted and add one more uncompacted
    val compact3 = (ids -- compact1 -- compact2).subsets(1).next() + newId1
    val newId3 = r.compact(compact3)

    Set(newId1, newId2) should not contain newId3

    // Compact the previously compacted and add one more compacted
    val compact4 = BitSet(newId2, newId3)
    val newId4 = r.compact(compact4)

    Set(newId1, newId2, newId3) should not contain newId4

    // Explode gives back the correct strings
    val originalIDs = (ids -- compact1 -- compact2 -- compact3 -- compact4).subsets(2).next()
    val idsToExplode = originalIDs + newId4
    r.explode(idsToExplode) should equal(
      compact1.flatMap(r.lookup) ++
        compact2.flatMap(r.lookup) ++
        compact3.flatMap(r.lookup) ++
        compact4.flatMap(r.lookup) ++
        originalIDs.flatMap(r.lookup)
    )
    r.explodedBitSet(idsToExplode) should equal(compact1 ++ compact2 ++ compact3 - newId1 ++ originalIDs)
  }
}
