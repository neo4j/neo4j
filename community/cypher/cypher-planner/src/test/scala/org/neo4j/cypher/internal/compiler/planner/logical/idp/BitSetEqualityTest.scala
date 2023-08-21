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
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink

import scala.collection.immutable.BitSet

class BitSetEqualityTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

  test("The way {128} is constructed should not affect equality") {
    // apply, via newBuilder, creates a mutable array of words under the hood, initially: [0].
    // It grows the array by doubling its size repeatedly until the new element can be added, if needed.
    // In the case of 128, which needs 3 words, it ends up with 4 words: [0, 0, 1, 0].
    val bitSetWithTrailingZeros = BitSet.apply(128)
    // incl on the other hand creates a new array exactly as large as needed, here: [0, 0, 1].
    val bitSet = BitSet.empty.incl(128)
    // Both [0, 0, 1, 0] and [0, 0, 1] represent the exact same set, and so should be considered equal and return the same hash code.
    BitSetEquality.equalBitSets(bitSetWithTrailingZeros, bitSet) shouldBe true
    BitSetEquality.hashCode(bitSetWithTrailingZeros) shouldEqual BitSetEquality.hashCode(bitSet)
  }

  test("Fast bit-set equality should produce the same result as the stock sorted set version") {
    forAll { (bitMask1: BitMaskWithTrailingZeros, bitMask2: BitMaskWithTrailingZeros) =>
      val bitSet1 = bitMask1.toBitSetWithTrailingZeros
      val bitSet2 = bitMask2.toBitSetWithTrailingZeros
      BitSetEquality.equalBitSets(bitSet1, bitSet2) should be(bitSet1.equals(bitSet2))
    }
  }

  test("Fast equality should respect reflexivity") {
    forAll { bitMask: BitMaskWithTrailingZeros =>
      val bitSet = bitMask.toBitSetWithTrailingZeros
      BitSetEquality.equalBitSets(bitSet, bitSet) shouldBe true
    }
  }

  test("Trailing zeros in the bit-mask should not affect equality") {
    forAll { bitMask: BitMaskWithTrailingZeros =>
      val bitSet = bitMask.toBitSet
      val bitSetWithTrailingZeros = bitMask.toBitSetWithTrailingZeros

      BitSetEquality.equalBitSets(bitSet, bitSetWithTrailingZeros) shouldBe true
      BitSetEquality.equalBitSets(bitSetWithTrailingZeros, bitSet) shouldBe true
    }
  }

  test("Trailing zeros in the bit-mask should not affect hash-code") {
    forAll { bitMask: BitMaskWithTrailingZeros =>
      val bitSet = bitMask.toBitSet
      val bitSetWithTrailingZeros = bitMask.toBitSetWithTrailingZeros

      BitSetEquality.hashCode(bitSetWithTrailingZeros) shouldEqual BitSetEquality.hashCode(bitSet)
    }
  }
}

case class BitMaskWithTrailingZeros(bitMask: BitMask, numberOfTrailingZeros: NumberOfTrailingZeros) {
  def toBitSet: BitSet = BitSet.fromBitMaskNoCopy(bitMask.words)

  def toBitSetWithTrailingZeros: BitSet =
    BitSet.fromBitMaskNoCopy(bitMask.words ++ numberOfTrailingZeros.toArray)
}

object BitMaskWithTrailingZeros {

  implicit lazy val arbitraryBitMaskWithTrailingZeros: Arbitrary[BitMaskWithTrailingZeros] =
    Arbitrary(genBitMaskWithTrailingZeros)

  lazy val genBitMaskWithTrailingZeros: Gen[BitMaskWithTrailingZeros] =
    for {
      bitMask <- BitMask.genBitMask
      numberOfTrailingZeros <- NumberOfTrailingZeros.genNumberOfTrailingZeros
    } yield BitMaskWithTrailingZeros(bitMask, numberOfTrailingZeros)

  implicit lazy val shrinkBitMaskWithTrailingZeros: Shrink[BitMaskWithTrailingZeros] =
    Shrink.xmap(
      Function.tupled(BitMaskWithTrailingZeros.apply(_, _)),
      bitMask => (bitMask.bitMask, bitMask.numberOfTrailingZeros)
    )
}

case class NumberOfTrailingZeros(value: Int) extends AnyVal {
  def toArray: Array[Long] = Array.fill(value)(0L)
}

object NumberOfTrailingZeros {

  implicit lazy val arbitraryNumberOfTrailingZeros: Arbitrary[NumberOfTrailingZeros] =
    Arbitrary(genNumberOfTrailingZeros)

  lazy val genNumberOfTrailingZeros: Gen[NumberOfTrailingZeros] =
    Gen.chooseNum(0, 5).map(NumberOfTrailingZeros.apply)

  implicit lazy val shrinkNumberOfTrailingZeros: Shrink[NumberOfTrailingZeros] =
    Shrink(zeros => Stream.range(0, zeros.value).map(NumberOfTrailingZeros.apply))
}

case class BitMask(words: Array[Long]) extends AnyVal {
  override def toString: String = words.map(word => s"0x${word.toHexString}L").mkString("[", ", ", "]")
}

object BitMask {
  implicit lazy val arbitraryBitMask: Arbitrary[BitMask] = Arbitrary(genBitMask)
  lazy val genBitMask: Gen[BitMask] = Arbitrary.arbitrary[Array[Long]].map(BitMask.apply)
  implicit lazy val shrinkBitMask: Shrink[BitMask] = Shrink.xmap(BitMask.apply, _.words)
}
