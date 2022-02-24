/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.generator.AstGenerator.tuple
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalacheck.Gen
import org.scalacheck.Gen.listOf
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable.BitSet

class BitSetEqualityTest extends CypherFunSuite with ScalaCheckDrivenPropertyChecks {

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

  private def bitset: Gen[BitSet] = for {
    list <- listOf(Gen.choose(0, 1000))
  } yield BitSet(list: _*)

  private def bitsetPair: Gen[(BitSet, BitSet)] = tuple(bitset, bitset)

  test("equality reflexivity") {
    forAll(bitset) { bs =>
      val bs2 = BitSet(bs.toSeq: _*)
      BitSetEquality.equalBitSets(bs, bs) should be(true)
      BitSetEquality.equalBitSets(bs, bs2) should be(true)
      BitSetEquality.equalBitSets(bs2, bs) should be(true)
    }
  }

  test("equality correctness") {
    forAll(bitsetPair) {
      case (bs1, bs2) => BitSetEquality.equalBitSets(bs1, bs2) should be(bs1.equals(bs2))
    }
  }

  test("hashCode correctness") {
    forAll(bitset) { bs1 =>
      val bs2 = BitSet(bs1.toSeq: _*)
      BitSetEquality.hashCode(bs1) should equal(BitSetEquality.hashCode(bs2))
    }
  }
}
