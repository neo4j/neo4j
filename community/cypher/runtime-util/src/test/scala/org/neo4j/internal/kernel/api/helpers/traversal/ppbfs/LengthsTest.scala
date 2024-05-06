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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LengthsTest extends CypherFunSuite {

  test("set & get") {
    val lengths = new Lengths

    Lengths.Type.values().foreach { lengthType =>
      lengths.set(1, lengthType)
      lengths.get(1, lengthType) shouldBe true
    }
  }

  test("clear") {
    val lengths = new Lengths

    Lengths.Type.values().foreach { lengthType =>
      lengths.set(1, lengthType)
      lengths.clear(1, lengthType)
      lengths.get(1, lengthType) shouldBe false
    }
  }

  test("next") {
    val lengths = new Lengths

    Lengths.Type.values().foreach { lengthType =>
      lengths.next(0, lengthType) shouldBe Lengths.NONE

      lengths.set(1, lengthType)
      lengths.set(3, lengthType)

      lengths.next(1, lengthType) shouldBe 1
      lengths.next(2, lengthType) shouldBe 3
    }
  }

  test("min") {
    val lengths = new Lengths

    Lengths.Type.values().foreach { lengthType =>
      lengths.set(1, lengthType)
      lengths.set(3, lengthType)

      lengths.min(lengthType) shouldBe 1
    }
  }

  test("isEmpty") {
    val lengths = new Lengths

    Lengths.Type.values().foreach { lengthType =>
      lengths.isEmpty(lengthType) shouldBe true
      lengths.set(1, lengthType)
      lengths.isEmpty(lengthType) shouldBe false
    }
  }
}
