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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.helpers.IterableHelper.RichIterableOnce
import org.neo4j.cypher.internal.compiler.helpers.IterableHelper.sequentiallyGroupBy
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class IterableHelperTest extends AnyFunSuite with Matchers with CypherScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  test("distinct values grouped by identity round-trip") {
    forAll { (distinctValues: Set[Int]) =>
      val values = distinctValues.toSeq
      sequentiallyGroupBy[Int, Int, Seq, Seq](values)(identity).flatMap(_._2) shouldEqual values
    }
  }

  test("previously grouped values round-trip") {
    forAll { (values: List[String]) =>
      val groups = values.groupBy(_.length).toSeq
      sequentiallyGroupBy[String, Int, Seq, Seq](groups.flatMap(_._2))(_.length) shouldEqual groups
    }
  }

  test("toMap . groupByOrder == groupBy") {
    forAll { (withDuplicates: WithDuplicates[Int]) =>
      sequentiallyGroupBy[Int, Int, Seq, Seq](withDuplicates.values)(
        _ % 5
      ).toMap shouldEqual withDuplicates.values.groupBy(_ % 5)
    }
  }

  test("for a sorted input, grouped keys are sorted") {
    forAll { (values: List[String]) =>
      val groups = sequentiallyGroupBy[String, Int, List, List](values.sortBy(_.length))(_.length)
      withClue(groups) {
        groups.map(_._1) shouldBe sorted
      }
    }
  }

  test("traverse on an empty input") {
    Seq.empty[String].traverse(_.headOption) shouldEqual Some(Seq.empty)
  }

  test("traverse on a single item list that returns Some") {
    Seq("foo").traverse(_.headOption) shouldEqual Some(Seq('f'))
  }

  test("traverse on a single item list that returns None") {
    Seq("").traverse(_.headOption) shouldEqual None
  }

  test("traverse on a multi item list that return all Some") {
    Seq("foo", "bar", "zak").traverse(_.headOption) shouldEqual Some(Seq('f', 'b', 'z'))
  }

  test("traverse on a multi item list that return all None") {
    Seq("", "", "").traverse(_.headOption) shouldEqual None
  }

  test("traverse on a multi item list that return Some and None") {
    Seq("foo", "", "bar").traverse(_.headOption) shouldEqual None
  }

  test("traverse - no None - random test") {
    forAll(Gen.listOf(Gen.nonEmptyListOf(Arbitrary.arbitrary[Char]))) { (values: List[List[Char]]) =>
      values.traverse(_.headOption) shouldEqual Some(values.map(_.head))
    }
  }

  val twoNonEmptyLists: Gen[(List[List[Char]], List[List[Char]])] = for {
    a <- Gen.listOf(Gen.nonEmptyListOf(Arbitrary.arbitrary[Char]))
    b <- Gen.listOf(Gen.nonEmptyListOf(Arbitrary.arbitrary[Char]))
  } yield (a, b)

  test("traverse - with None - random test") {
    forAll(twoNonEmptyLists) {
      case (valuesA, valuesB) =>
        val valuesWithNoneMixedIn = (valuesA :+ List.empty[Char]) ++ valuesB
        valuesWithNoneMixedIn.traverse(_.headOption) shouldEqual None
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
