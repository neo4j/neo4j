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
package org.neo4j.internal.collector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataCollectorMatchersTest extends AnyFunSuite {

  test("arraySafeEquals") {
    DataCollectorMatchers.arraySafeEquals(Array(), Seq()) shouldBe true
    DataCollectorMatchers.arraySafeEquals(Array(1), Seq(1)) shouldBe true
    DataCollectorMatchers.arraySafeEquals(Array(), Seq(1)) shouldBe false
    DataCollectorMatchers.arraySafeEquals(Array(1), Seq()) shouldBe false
    DataCollectorMatchers.arraySafeEquals(Array(1), Seq(2)) shouldBe false
    DataCollectorMatchers.arraySafeEquals(Array(Array(1)), Array(Seq(1))) shouldBe true
    DataCollectorMatchers.arraySafeEquals(Seq(Array(1)), Seq(Seq(1))) shouldBe false

    val equalTuples =
      Seq((1, 1), ("1", "1"), (Array(3, 4), Seq(3, 4)), (Seq(5), Array(5)), (Array(Array(1)), Array(Seq(1))))
    val equalData = subsequences(equalTuples)
    for (testDatum <- equalData) {
      val firstSequence = testDatum.map(_._1).toArray
      val secondSequence = testDatum.map(_._2).toArray
      withClue(firstSequence) {
        DataCollectorMatchers.arraySafeEquals(firstSequence, secondSequence) shouldBe true
      }
    }
    val unequalData = for {
      lhs <- subsequences(equalTuples.map(_._1))
      rhs <- subsequences(equalTuples.map(_._2))
      if !equalData.contains(lhs.zip(rhs))
    } yield (lhs, rhs)

    for (testDatum <- unequalData) {
      val firstSequence = testDatum._1.toArray
      val secondSequence = testDatum._2.toArray
      withClue(firstSequence) {
        DataCollectorMatchers.arraySafeEquals(firstSequence, secondSequence) shouldBe false
      }
    }
  }

  def subsequences[T](sequence: Seq[T]): Seq[Seq[T]] = {
    sequence match {
      case init :+ last =>
        val initSubsequences = subsequences(init)
        initSubsequences.map(_ :+ last) ++ initSubsequences
      case _ => Seq(Seq.empty)
    }
  }
}
