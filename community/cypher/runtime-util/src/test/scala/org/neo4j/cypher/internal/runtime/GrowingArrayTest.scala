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
import org.neo4j.memory.LocalMemoryTracker

class GrowingArrayTest extends CypherFunSuite {

  test("empty") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.hasNeverSeenData shouldBe true
    x.foreach(l => fail("There should not be any elements"))
  }

  test("set and get") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.set(0, "a")
    x.get(0) shouldBe "a"

    x.set(0, "b")
    x.get(0) shouldBe "b"

    x.set(1, "c")
    x.get(0) shouldBe "b"
    x.get(1) shouldBe "c"
  }

  test("set a lot") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)

    for (i <- 0 until 1000) {
      x.set(i, "" + i)
    }

    x.get(265) shouldBe "265"
    x.get(42) shouldBe "42"
    x.get(999) shouldBe "999"
  }

  test("foreach") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    for (i <- 0 until 10) {
      x.set(i, "" + i)
    }

    val builder = Seq.newBuilder[String]
    x.foreach(str => builder += str)
    builder shouldBe Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
  }

  test("foreach ignores gaps and nulls") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.set(0, "0")
    x.set(2, "2")
    x.set(3, null)
    x.set(4, "4")

    val builder = Seq.newBuilder[String]
    x.foreach(str => builder += str)
    builder shouldBe Seq("0", "2", "4")
  }

  test("hasNeverSeenData") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.hasNeverSeenData shouldBe true

    x.set(0, "a")
    x.hasNeverSeenData shouldBe false

    x.set(0, null)
    x.hasNeverSeenData shouldBe false
  }

  test("set on an large out-of-bounds index") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.set(1234, "a")
    x.get(1234) shouldBe "a"
  }

  test("isDefinedAt") {
    val x = new GrowingArray[String](EmptyMemoryTracker.INSTANCE)
    x.isDefinedAt(1234) shouldBe false

    x.set(1234, "a")
    x.isDefinedAt(1234) shouldBe true
    x.isDefinedAt(0) shouldBe false
  }

  test("should allocate memory on creation") {
    val memoryTracking = new LocalMemoryTracker()
    new GrowingArray[String](memoryTracking)

    memoryTracking.estimatedHeapMemory() shouldBe >(0L)
  }

  test("should allocate when adding objects") {
    // given
    val memoryTracking = new LocalMemoryTracker()
    val x = new GrowingArray[String](memoryTracking)
    val initMemory = memoryTracking.estimatedHeapMemory()

    // when
    (0 to 16).foreach(i => x.set(i, i.toString))

    // then
    memoryTracking.estimatedHeapMemory() should be > initMemory
  }
}
