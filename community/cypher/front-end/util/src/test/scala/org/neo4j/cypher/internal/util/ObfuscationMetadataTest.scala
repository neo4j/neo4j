/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ObfuscationMetadataTest extends AnyFunSuite with Matchers {

  test("normalize sorts by start and de-duplicates identical offsets") {
    val m = ObfuscationMetadata(
      Vector(LiteralOffset(5, 1, Some(2)), LiteralOffset(1, 1, Some(3)), LiteralOffset(5, 1, Some(2))),
      Vector.empty,
      Set.empty
    )
    m.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(1, 1, Some(3)), LiteralOffset(5, 1, Some(2)))
  }

  test("normalize keeps the widest span when two known lengths collide at one start (fail closed)") {
    val m = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, Some(3)), LiteralOffset(3, 1, Some(7))),
      Vector.empty,
      Set.empty
    )
    m.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, Some(7)))
  }

  test("normalize prefers a known length over an unknown-length fallback at the same start") {
    val m = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, None), LiteralOffset(3, 1, Some(4))),
      Vector.empty,
      Set.empty
    )
    m.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, Some(4)))
  }

  test("normalize keeps a None offset only when no known length exists at that start") {
    val m = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, None)),
      Vector.empty,
      Set.empty
    )
    m.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, None))
  }

  test("both views are normalized independently") {
    val m = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, Some(7)), LiteralOffset(3, 1, Some(3))),
      Vector(LiteralOffset(9, 1, Some(1)), LiteralOffset(2, 1, Some(2))),
      Set("p")
    )
    m.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, Some(7)))
    m.allLiteralOffsets shouldBe Vector(LiteralOffset(2, 1, Some(2)), LiteralOffset(9, 1, Some(1)))
    m.sensitiveParameterNames shouldBe Set("p")
  }

  test("merge re-normalizes both views and unions the parameter names") {
    val a = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, Some(3))),
      Vector(LiteralOffset(3, 1, Some(3))),
      Set("a")
    )
    val b = ObfuscationMetadata(
      Vector(LiteralOffset(3, 1, Some(7))),
      Vector(LiteralOffset(3, 1, Some(7))),
      Set("b")
    )
    val merged = a.merge(b)
    merged.sensitiveLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, Some(7)))
    merged.allLiteralOffsets shouldBe Vector(LiteralOffset(3, 1, Some(7)))
    merged.sensitiveParameterNames shouldBe Set("a", "b")
  }

  test("empty has both views empty and isEmpty") {
    val e = ObfuscationMetadata.empty()
    e.sensitiveLiteralOffsets shouldBe empty
    e.allLiteralOffsets shouldBe empty
    e.sensitiveParameterNames shouldBe empty
    e.isEmpty shouldBe true
  }
}
