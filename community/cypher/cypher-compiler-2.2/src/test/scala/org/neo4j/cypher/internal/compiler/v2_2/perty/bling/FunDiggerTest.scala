/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import org.neo4j.cypher.internal.commons.CypherFunSuite

import scala.reflect.ClassTag

class FunDiggerTest extends CypherFunSuite {
  type TestDrill = Drill[Any, Any, String]
  type TestDigger = FunDigger.type#AbstractFunDigger[Any, Any, String]

  test("Constructs empty digger") {
    val digger: TestDigger = FunDigger.empty
    val result = digger((x: Any) => Some(x.toString))("String")

    result should equal(None)
  }

  test("Constructs and applies digger based on single drill to matching value") {
    val drill: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val digger = FunDigger.fromSingleDrill[Any, Any, String](drill)
    val result = digger(toStringExtractor)(1)

    result should equal(Some("X"))
  }

  test("Constructs and applies digger based on single drill to non-matching value") {
    val drill: TestDrill = mkDrill() { case i: Int =>  inner => "X" }
    val digger = FunDigger.fromSingleDrill[Any, Any, String](drill)
    val result = digger(toStringExtractor)(true)

    result should equal(None)
  }

  test("Constructs and applies digger based on two drills to non-matching value") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case b: Boolean => inner => "Y" }
    val digger = FunDigger(drill1, drill2)
    val result = digger(toStringExtractor)("Z")

    result should equal(None)
  }

  test("Constructs and applies digger based on two drills to matching value for first drill") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case b: Boolean => inner => "Y" }
    val digger = FunDigger(drill1, drill2)
    val result = digger((x: Any) => Some(x.toString))(1)

    result should equal(Some("X"))
  }

  test("Constructs and applies digger based on two drills to matching value for second drill") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case b: Boolean => inner => "Y" }
    val digger = FunDigger(drill1, drill2)
    val result = digger(toStringExtractor)(true)

    result should equal(Some("Y"))
  }

  test("Earlier drill shadows later drill") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case i: Int => inner => "Y" }
    val digger = FunDigger(drill1, drill2)
    val result = digger(toStringExtractor)(1)

    result should equal(Some("X"))
  }

  test("FunDrills compose with other FunDrills") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case i: Int => inner => "Y" }
    val drill3: TestDrill = mkDrill() { case i: Int => inner => "Z" }

    val digger1 = FunDigger(drill1)
    val digger2 = FunDigger(drill2, drill3)

    val composedDigger = digger1 ++ digger2

    composedDigger.drills should equal(Seq(drill1, drill2, drill3))
  }

  test("FunDrills compose with other non-FunDrill drills") {
    val drill1: TestDrill = mkDrill() { case i: Int => inner => "X" }
    val drill2: TestDrill = mkDrill() { case i: Int => inner => "Y" }
    val drill3: TestDrill = mkDrill() { case i: Int => inner => "Z" }

    val digger1 = FunDigger(drill1)
    val digger2 = FunDigger(drill2, drill3)
    val digger3 = MockedTestDigger(digger2.drill)

    val composedDigger = digger1 orElse digger3

    composedDigger.drills should equal(Seq(drill1, digger2.drill))
  }

  test("FunDrills can be mapped with before") {
    val drill: TestDrill = mkDrill() { case i: Int => inner => i.toString }
    val digger: TestDigger = FunDigger(drill).mapInput[Any] { case _ => 2 }
    val result = digger(toStringExtractor)(1)

    result should equal(Some("2"))
  }

  test("FunDrills can be mapped with map") {
    val drill: TestDrill = mkDrill() { case x => (inner: Any => String) => inner("!" + x.toString) }
    val digger: TestDigger = FunDigger(drill).mapMembers(_.toString.length)
    val result = digger(toStringExtractor)(123)

    result should equal(Some("4"))
  }

  test("FunDrills can be mapped with andThen") {
    val drill: TestDrill = mkDrill() { case x => (inner: Any => String) => inner("!" + x.toString) }
    val digger: TestDigger = FunDigger(drill)
    val result = digger.mapOutput((x: String) => x.reverse)(toStringExtractor)(123)

    result should equal(Some("321!"))
  }

  test("FunDrills extract over children") {
    case class X(i: Int, x: Option[X])
    val drill1: TestDrill = mkDrill() {
      case Some(X(i, opt)) => (inner: Any => String) =>
        s"X(${inner(i)}, ${inner(opt)})"
    }
    val drill2: TestDrill = mkDrill() { case x => inner => "!" + x.toString }
    val digger: TestDigger = FunDigger(drill1, drill2)

    val result = digger(digger.fixPoint)(Some(X(1, Some(X(2, None)))))

    result should equal(Some("X(!1, X(!2, !None))"))
  }

  case class MockedTestDigger[-A](drill: TestDrill) extends Digger[A, Any, String] {
    def apply(extractor: Extractor[Any, String]) = _ => ???
    def fixPoint: Extractor[Any, String] = _ => ???
    def lift[A0: ClassTag] = ???
    def mapInput[A0: ClassTag](f: PartialFunction[A0, A]) = ???
    def orElse[A1 <: A : ClassTag](other: Digger[A1, Any, String]) = ???
    def mapOutput[O1 <: String](f: (String) => O1) = ???
    def mapMembers[M1](f: (Any) => M1) = ???
  }
}
