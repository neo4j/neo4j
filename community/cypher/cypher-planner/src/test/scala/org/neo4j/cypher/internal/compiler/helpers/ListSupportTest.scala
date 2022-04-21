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
package org.neo4j.cypher.internal.compiler.helpers

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class ListSupportTest extends FunSuite with Matchers with ScalaCheckPropertyChecks with ListSupport {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  test("group strings by length preserving order") {
    val strings = Seq("foo", "", "a", "bar", "", "b", "c")
    val groups = strings.sequentiallyGroupBy(_.length)
    val expected =
      Seq(
        3 -> Seq("foo", "bar"),
        0 -> Seq("", ""),
        1 -> Seq("a", "b", "c")
      )

    groups shouldEqual expected
  }

  test("distinct values grouped by identity round-trip") {
    forAll { distinctValues: Set[Int] =>
      val values = distinctValues.toSeq
      values.sequentiallyGroupBy(identity).flatMap(_._2) shouldEqual values
    }
  }

  test("previously grouped values round-trip") {
    forAll { values: List[String] =>
      val groups = values.groupBy(_.length).toSeq
      groups.flatMap(_._2).sequentiallyGroupBy(_.length) shouldEqual groups
    }
  }

  test("toMap . groupByOrder == groupBy") {
    forAll { withDuplicates: WithDuplicates[Int] =>
      withDuplicates.values.sequentiallyGroupBy(_ % 5).toMap shouldEqual withDuplicates.values.groupBy(_ % 5)
    }
  }

  test("for a sorted input, grouped keys are sorted") {
    forAll { values: List[String] =>
      val groups = values.sortBy(_.length).sequentiallyGroupBy(_.length)
      withClue(groups) {
        groups.map(_._1) shouldBe sorted
      }
    }
  }
}

case class WithDuplicates[A](values: Seq[A])

object WithDuplicates {
  implicit def arbitraryWithDuplicates[A: Arbitrary]: Arbitrary[WithDuplicates[A]] = Arbitrary(genWithDuplicates)

  def genWithDuplicates[A: Arbitrary]: Gen[WithDuplicates[A]] =
    for {
      values <- Arbitrary.arbitrary[Seq[A]]
      duplicates <- Gen.someOf(values)
      seed <- Arbitrary.arbitrary[Long]
      random = new Random(seed)
    } yield WithDuplicates(random.shuffle(values ++ duplicates))
}
