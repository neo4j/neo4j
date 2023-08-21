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
package org.neo4j.cypher.internal.util.test_helpers

import org.scalatest.TryValues

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class CypherScalaCheckDrivenPropertyChecksTest extends CypherFunSuite
    with CypherScalaCheckDrivenPropertyChecks
    with TryValues {

  test("should generate same number given the same initial seed") {
    def randomNumbers(): Seq[Int] = {
      setScalaCheckInitialSeed(123)

      val numbers = ArrayBuffer.empty[Int]
      forAll { n: Int => numbers += n }
      numbers.toSeq
    }

    randomNumbers() shouldEqual randomNumbers()
  }

  test("should report initial seed correctly in case of a test failure") {
    val initialSeed = 1234567890L
    setScalaCheckInitialSeed(initialSeed)

    val t = Try {
      forAll { _: Int => fail() }
    }

    t.failure.exception.getMessage should include regex s"Init Seed: $initialSeed$$"
  }
}
