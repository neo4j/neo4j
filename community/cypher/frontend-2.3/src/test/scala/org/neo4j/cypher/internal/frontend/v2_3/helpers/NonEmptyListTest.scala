/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NonEmptyListTest extends CypherFunSuite {

  import NonEmptyList._

  test("Should construct NonEmptyLists") {
    NonEmptyList(1) should equal(Last(1))
    NonEmptyList(1, 2) should equal(Fby(1, Last(2)))
  }

  test("Should build NonEmptyLists") {
    val builder = NonEmptyList.newBuilder[Int]

    builder.result() should equal(None)

    builder += 1

    builder.result() should equal(Some(NonEmptyList(1)))

    builder += 2

    builder.result() should equal(Some(NonEmptyList(1, 2)))

    builder.clear()

    builder.result() should equal(None)
  }

  test("Should construct builders via canBuildFrom") {
    canBuildFrom(Seq(1)).result() should equal(None)
    (canBuildFrom(Seq(1)) += 2).result() should equal(Some(NonEmptyList(2)))

    val result = Seq(1, 2, 3).map(_.toString): Option[NonEmptyList[String]]
    result should equal(Some(NonEmptyList("1", "2", "3")))
  }

  test("Should convert to NonEmptyList") {
    Seq().toNonEmptyListOption should equal(None)
    Seq(1).toNonEmptyListOption should equal(Some(Last(1)))
    Seq(1, 2).toNonEmptyListOption should equal(Some(Fby(1, Last(2))))
  }

  test("Should convert from NonEmptyList") {
    NonEmptyList(1).toSeq should equal(Seq(1))
    NonEmptyList(1).toSet should equal(Set(1))
    NonEmptyList(1).toList should equal(List(1))
    NonEmptyList(1).toIterable.toArray should equal(Array(1))

    NonEmptyList(1, 2, 2).toSeq should equal(Seq(1, 2, 2))
    NonEmptyList(1, 2, 2).toSet should equal(Set(1, 2))
    NonEmptyList(1, 2, 2).toList should equal(List(1, 2, 2))
    NonEmptyList(1, 2, 2).toIterable.toArray should equal(Array(1, 2, 2))
  }

  test("Should inspect single element NonEmptyList") {
    NonEmptyList(1).head should equal(1)
    NonEmptyList(1).tailOption should equal(None)
    NonEmptyList(1).hasTail should be(right = false)
    NonEmptyList(1).isLast should be(right = true)
  }

  test("Should inspect multi element NonEmptyList") {
    NonEmptyList(1, 2).head should equal(1)
    NonEmptyList(1, 2).tailOption should equal(Some(NonEmptyList(2)))
    NonEmptyList(1, 2).hasTail should be(right = true)
    NonEmptyList(1, 2).isLast should be(right = false)
  }

  test("Should prepend single element to NonEmptyLists") {
    2 +: NonEmptyList(1) should equal(NonEmptyList(2, 1))
  }

  test("should append single element to NonEmptyLists") {
    NonEmptyList(1) :+ 2 should equal(NonEmptyList(1, 2))
  }

  test("should append single element to longer NonEmptyLists") {
    NonEmptyList(1, 3, 4, 5, 6) :+ 2 should equal(NonEmptyList(1, 3, 4, 5, 6, 2))
  }

  test("Should map and prepend reversed to NonEmptyList") {
    NonEmptyList(2, 3).mapAndPrependReversedTo[Int, Int](_ + 1, NonEmptyList(1)) should equal(NonEmptyList(4, 3, 1))
    NonEmptyList(2, 3).mapAndPrependReversedTo[Int, Int](_ + 1, NonEmptyList(1, 2)) should equal(NonEmptyList(4, 3, 1, 2))
    NonEmptyList(2).mapAndPrependReversedTo[Int, Int](_ + 1, NonEmptyList(1, 2)) should equal(NonEmptyList(3, 1, 2))
    NonEmptyList(2).mapAndPrependReversedTo[Int, Int](_ + 1, NonEmptyList(1)) should equal(NonEmptyList(3, 1))
  }

  test("Should prepend multiple elements to NonEmptyLists") {
    Seq(2, 3).iterator ++: NonEmptyList(1) should equal(NonEmptyList(2, 3, 1))
    Seq(2, 3) ++: NonEmptyList(1) should equal(NonEmptyList(2, 3, 1))
    Seq(2) ++: NonEmptyList(1) should equal(NonEmptyList(2, 1))
    Seq(2) ++: NonEmptyList(1, 4) should equal(NonEmptyList(2, 1, 4))
    Seq(2, 3, 5) ++: NonEmptyList(1, 4) should equal(NonEmptyList(2, 3, 5, 1, 4))
  }

  test("Should concatenate NonEmptyLists") {
    (NonEmptyList(1) ++ NonEmptyList(4)) should equal(NonEmptyList(1, 4))
    (NonEmptyList(1) ++ NonEmptyList(4, 5 ,6)) should equal(NonEmptyList(1, 4, 5, 6))
    (NonEmptyList(1, 2, 3) ++ NonEmptyList(4)) should equal(NonEmptyList(1, 2, 3, 4))
    (NonEmptyList(1, 2, 3) ++ NonEmptyList(4, 5 ,6)) should equal(NonEmptyList(1, 2, 3, 4, 5, 6))
  }

  test("Should support foreach on NonEmptyLists") {
    var count = 0

    NonEmptyList(1).foreach { _ => count += 1 }

    count should equal(1)

    NonEmptyList(1, 2, 3, 4).foreach { _ => count += 2 }

    count should equal(9)
  }

  test("Should support filtering a NonEmptyList") {
    NonEmptyList(1).filter(_ == 0) should equal(None)
    NonEmptyList(1).filter(_ == 1) should equal(Some(NonEmptyList(1)))

    NonEmptyList(1, 2).filter(_ == 0) should equal(None)
    NonEmptyList(1, 3).filter(_ <= 2) should equal(Some(NonEmptyList(1)))
    NonEmptyList(1, 2).filter(_ >= 0) should equal(Some(NonEmptyList(1, 2)))
  }

  test("Should map NonEmptyList") {
    NonEmptyList(1).map(_ + 1) should equal(NonEmptyList(2))
    NonEmptyList(1, 2).map(_ + 1) should equal(NonEmptyList(2, 3))
  }

  test("Should collect from a NonEmptyList") {
    NonEmptyList(1).collect { case x if x == 1 => "Apa" } should equal(Some(NonEmptyList("Apa")))
    NonEmptyList(1, 2).collect { case x if x == 1 => "Apa" } should equal(Some(NonEmptyList("Apa")))
    NonEmptyList(1).collect { case x if x != 1 => "Apa" } should equal(None)
    NonEmptyList(1, 2).collect { case x if x != 1 => "Apa" } should equal(Some(NonEmptyList("Apa")))
  }

  test("Should reverseFlatMap a NonEmptyList") {
    NonEmptyList(1).reverseFlatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(2, 1))
    NonEmptyList(1, 4).reverseFlatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(5, 4, 2, 1))
    NonEmptyList(1, 4, 8).reverseFlatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(9, 8, 5, 4, 2, 1))
  }

  test("Should flatMap a NonEmptyList") {
    NonEmptyList(1).flatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(1, 2))
    NonEmptyList(1, 4).flatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(1, 2, 4, 5))
    NonEmptyList(1, 4, 8).flatMap(x => NonEmptyList(x, x + 1)) should equal(NonEmptyList(1, 2, 4, 5, 8, 9))
  }

  test("Should fold left over a NonEmptyList") {
    NonEmptyList(1).foldLeft(Seq.empty[Int])(_ :+ _) should equal(Seq(1))
    NonEmptyList(1, 2).foldLeft(Seq.empty[Int])(_ :+ _) should equal(Seq(1, 2))
  }

  test("Should left reduce a NonEmptyList") {
    NonEmptyList(23).reduceLeft(_ + _) should equal(23)
    NonEmptyList(42, 23).reduceLeft(_ - _) should equal(19)
    NonEmptyList(42, 23, 10).reduceLeft(_ - _) should equal(9)
  }

  test("Should group a NonEmptyList by a key for each element") {
    NonEmptyList(1).groupBy((x: Int) => x / 2) should equal(Map(0 -> NonEmptyList(1)))
    NonEmptyList(1, 2).groupBy((x: Int) => x / 2) should equal(Map(0 -> NonEmptyList(1), 1 -> NonEmptyList(2)))
    NonEmptyList(1, 2, 3).groupBy((x: Int) => x / 2) should equal(Map(0 -> NonEmptyList(1), 1 -> NonEmptyList(2, 3)))
  }

  test("Should find min in a NonEmptyList") {
    NonEmptyList(1).min should equal(1)
    NonEmptyList(1, 2).min should equal(1)
    NonEmptyList(4, 1, 2).min should equal(1)
    NonEmptyList(4, 1).min should equal(1)
  }

  test("Should find max in a NonEmptyList") {
    NonEmptyList(10).max should equal(10)
    NonEmptyList(10, 2).max should equal(10)
    NonEmptyList(4, 10, 2).max should equal(10)
    NonEmptyList(4, 10).max should equal(10)
  }

  test("Should partition a NonEmptyList") {
    NonEmptyList(1).partition { x => if (x == 1) Left(x) else Right(x) } should equal(Left(NonEmptyList(1) -> None))
    NonEmptyList(1).partition { x => if (x != 1) Left(x) else Right(x) } should equal(Right(None -> NonEmptyList(1)))

    NonEmptyList(1, 2, 3).partition { x => if (x == 1) Left(x) else Right(x) } should equal(Left(NonEmptyList(1) -> Some(NonEmptyList(2, 3))))
    NonEmptyList(1, 2, 3).partition { x => if (x != 1) Left(x) else Right(x) } should equal(Right(Some(NonEmptyList(2, 3)) -> NonEmptyList(1)))

    NonEmptyList(1, 2, 3).partition { x => if (x != 0) Left(x) else Right(x) } should equal(Left(NonEmptyList(1, 2, 3) -> None))
    NonEmptyList(1, 2, 3).partition { x => if (x == 0) Left(x) else Right(x) } should equal(Right(None -> NonEmptyList(1, 2, 3)))
  }

  test("exists should work as expected") {
    NonEmptyList(1).exists(_ == 2) should be(false)
    NonEmptyList(1).exists(_ == 1) should be(true)
    NonEmptyList(1, 2, 3).exists(_ == 4) should be(false)
    NonEmptyList(1, 2, 3).exists(_ < 4) should be(true)
    NonEmptyList(1, 2, 3).exists(_ >= 3) should be(true)
  }

  test("forall should work as expected") {
    NonEmptyList(1).forall(_ == 2) should be(false)
    NonEmptyList(1).forall(_ == 1) should be(true)
    NonEmptyList(1, 2, 3).forall(_ == 4) should be(false)
    NonEmptyList(1, 2, 3).forall(_ < 4) should be(true)
    NonEmptyList(1, 2, 3).forall(_ >= 3) should be(false)
  }
}
