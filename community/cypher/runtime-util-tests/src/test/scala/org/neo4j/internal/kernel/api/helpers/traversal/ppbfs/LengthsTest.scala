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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite

class LengthsTest extends RuntimeUtilTestSuite {

  test("seen and validated in trail-mode") {
    val lengths = Lengths.trailMode()
    lengths.markAsSeen(1)
    lengths.seenAt(1) shouldBe true
    lengths.validatedAt(1) shouldBe false
    lengths.markAsValidated(1)
    lengths.seenAt(1) shouldBe true
    lengths.validatedAt(1) shouldBe true
  }

  test("seen and validated in walk-mode") {
    val lengths = Lengths.walkMode()
    lengths.markAsSeen(1)
    lengths.seenAt(1) shouldBe true
    lengths.validatedAt(1) shouldBe true
    lengths.markAsValidated(1)
    lengths.seenAt(1) shouldBe true
    lengths.validatedAt(1) shouldBe true
  }

  test("clearSeen in trail mode") {
    val lengths = Lengths.trailMode()
    lengths.markAsSeen(1)
    lengths.clearSeen(1)
    lengths.seenAt(1) shouldBe false
  }

  test("clearSeen in walk mode") {
    val lengths = Lengths.walkMode()
    lengths.markAsSeen(1)
    lengths.clearSeen(1)
    lengths.seenAt(1) shouldBe false
  }

  test("next trail mode") {
    val lengths = Lengths.trailMode()

    lengths.nextSeen(0) shouldBe Lengths.NONE

    lengths.markAsSeen(1)
    lengths.markAsSeen(3)

    lengths.nextSeen(1) shouldBe 1
    lengths.nextSeen(2) shouldBe 3
  }

  test("next walk mode") {
    val lengths = Lengths.walkMode()

    lengths.nextSeen(0) shouldBe Lengths.NONE

    lengths.markAsSeen(1)
    lengths.markAsSeen(3)

    lengths.nextSeen(1) shouldBe 1
    lengths.nextSeen(2) shouldBe 3
  }
}
