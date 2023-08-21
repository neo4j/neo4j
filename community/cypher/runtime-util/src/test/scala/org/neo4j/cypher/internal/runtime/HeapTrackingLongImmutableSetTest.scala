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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker

import scala.util.Random

class HeapTrackingLongImmutableSetTest extends CypherFunSuite {

  test("empty set should be empty") {
    val emptySet = HeapTrackingLongImmutableSet.emptySet(EmptyMemoryTracker.INSTANCE)

    emptySet.size shouldBe 0
    emptySet.contains(Random.nextLong()) shouldBe false
  }

  test("growing set") {
    // given
    val item1 = 1
    val item2 = 2
    val item3 = 3
    val item4 = 4
    val item5 = 5
    val emptySet = heapTrackingSet()

    // when
    val set1 = emptySet + item1
    val set2 = set1 + item2
    val set3 = set2 + item3
    val set4 = set3 + item4
    val set5 = set4 + item5

    // then
    set5.size shouldBe 5
    set5.contains(item1) shouldBe true
    set5.contains(item2) shouldBe true
    set5.contains(item3) shouldBe true
    set5.contains(item4) shouldBe true
    set5.contains(item5) shouldBe true

    set4.size shouldBe 4
    set4.contains(item1) shouldBe true
    set4.contains(item2) shouldBe true
    set4.contains(item3) shouldBe true
    set4.contains(item4) shouldBe true
    set4.contains(item5) shouldBe false

    set3.size shouldBe 3
    set3.contains(item1) shouldBe true
    set3.contains(item2) shouldBe true
    set3.contains(item3) shouldBe true
    set3.contains(item4) shouldBe false
    set3.contains(item5) shouldBe false

    set2.size shouldBe 2
    set2.contains(item1) shouldBe true
    set2.contains(item2) shouldBe true
    set2.contains(item3) shouldBe false
    set2.contains(item4) shouldBe false
    set2.contains(item5) shouldBe false

    set1.size shouldBe 1
    set1.contains(item1) shouldBe true
    set1.contains(item2) shouldBe false
    set1.contains(item3) shouldBe false
    set1.contains(item4) shouldBe false
    set1.contains(item5) shouldBe false

    emptySet.size shouldBe 0
    emptySet.contains(item1) shouldBe false
    emptySet.contains(item2) shouldBe false
    emptySet.contains(item3) shouldBe false
    emptySet.contains(item4) shouldBe false
    emptySet.contains(item5) shouldBe false
  }

  test("adding different values to different set") {
    val set = heapTrackingSet(1, 2, 3, 4, 5)
    val appendedSetA = set + 6
    val appendedSetB = set + 7

    appendedSetA.contains(6) shouldBe true
    appendedSetA.contains(7) shouldBe false

    appendedSetB.contains(6) shouldBe false
    appendedSetB.contains(7) shouldBe true
  }

  test("adding same element should be noop") {
    (1 to 128).foreach(last => {
      val set = heapTrackingSet(1L to last: _*)
      val newSet = set + last
      newSet should be theSameInstanceAs set
    })
  }

  test("stress test") {
    var trackingSet = heapTrackingSet()
    var referenceSet = Set.empty[Long]
    var i = 0
    while (i < 1000) {
      val item = Random.nextLong()
      val newTrackingSet = trackingSet + item
      val newReferenceSet = referenceSet + item
      newTrackingSet.contains(item) shouldBe true
      newTrackingSet.size should equal(newReferenceSet.size)
      newReferenceSet.foreach(l => newTrackingSet.contains(l) shouldBe true)
      // make sure old sets still valid
      trackingSet.size should equal(referenceSet.size)
      referenceSet.foreach(l => trackingSet.contains(l) shouldBe true)
      referenceSet = newReferenceSet
      trackingSet = newTrackingSet
      i += 1
    }
  }

  test("stress test branched") {
    var i = 0
    while (i < 100) {
      val size = Random.between(0, 1000)

      val trackingSet = heapTrackingSet(1L to size: _*)
      val referenceSet = Set(1L to size: _*)
      val newItem1 = Random.nextLong()
      val newItem2 = Random.nextLong()
      val newTrackingSet1 = trackingSet + newItem1
      val newReferenceSet1 = referenceSet + newItem1
      val newTrackingSet2 = trackingSet + newItem2
      val newReferenceSet2 = referenceSet + newItem2

      newTrackingSet1.contains(newItem1) shouldBe true
      newTrackingSet2.contains(newItem2) shouldBe true
      newTrackingSet1.contains(newItem2) should equal(newReferenceSet1.contains(newItem2))
      newTrackingSet2.contains(newItem1) should equal(newReferenceSet2.contains(newItem1))
      // make sure old sets still valid
      trackingSet.size should equal(referenceSet.size)
      referenceSet.foreach(l => trackingSet.contains(l) shouldBe true)
      i += 1
    }
  }

  private def heapTrackingSet(elements: Long*) = {
    val empty = HeapTrackingLongImmutableSet.emptySet(EmptyMemoryTracker.INSTANCE)
    elements.foldLeft(empty)((current, element) => current + element)
  }

}
