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
package org.neo4j.cypher.internal.runtime

import java.util.concurrent.ThreadLocalRandom

import org.github.jamm.MemoryMeter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.LocalMemoryTracker
//NOTE: this test must be run with a javaagent, the easiest way to accomplish that
// is by running as a junit test rather than a scalatest.
class GrowingArrayTrackingTest extends CypherFunSuite {
  private val random = ThreadLocalRandom.current()

  test("add and measure") {
    //give
    val tracker = new LocalMemoryTracker()
    val array = new GrowingArray[java.lang.Long](tracker)
    val meter = new MemoryMeter()
    val iterations = random.nextInt(10, 1000)
    (0 until iterations).foreach(i => array.set(i, i))

    //when
    val itemSize = meter.measure(1L) * iterations
    val actualSize = meter.measureDeep(array) - meter.measureDeep(tracker) - itemSize

    //then
    actualSize should equal(tracker.estimatedHeapMemory())
    array.close()
    //check so that we are not leaking memory
    tracker.estimatedHeapMemory() should equal(0)
  }

}
