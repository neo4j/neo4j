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
import org.neo4j.cypher.internal.frontend.v2_3.perty.Extractor
import org.scalatest.{FunSuite, Matchers}

class SimpleExtractorTest extends FunSuite with Matchers {

  import Extractor._

  test("Creating simple extractor from total function") {
    val extractor = extract[Foo, String] { f => Some(f.toString) }
    val input = Bar("Ava")
    val result = extractor(input)

    result should equal(Some(input.toString))
  }

  test("Creating simple extractor from partial function") {
    val extractor = pick[Foo, String] { case Bar(name) => name }
    val bar = Bar("Ava")
    val baz = Baz(1, bar)

    extractor(bar) should equal(Some(bar.name))
    extractor(baz) should equal(None)
  }

  test("Simple extractor andThen (1)") {
    val barExtractor = extract[Bar, String] { b => Some(b.name) }
    val revExtractor = barExtractor.andThen(extract[String, String] { s => Some(s.reverse) } )

    revExtractor(Bar("!avA")) should equal(Some("Ava!"))
  }

  test("Simple extractor andThen (2)") {
    val barExtractor = extract[Bar, String] { b => Some(b.name) }
    val intExtractor = barExtractor.andThen(extract[String, Int] { s => Some(s.length) } )

    intExtractor(Bar("Hello!")) should equal(Some(6))
  }

  test("Simple extractor orElse") {
    val barExtractor = pick[Foo, String] { case _: Bar => "Bar" }
    val bazExtractor = pick[Foo, String] { case _: Baz => "Baz" }
    val extractor = barExtractor orElse bazExtractor

    extractor(Bar("x")) should equal(Some("Bar"))
    extractor(Baz(1, Bar("y"))) should equal(Some("Baz"))
  }

  test("Lift simple extractor") {
    val extractor = fromInput[List[Integer], List[Number]].lift[List[AnyRef]]

    extractor[List[Integer]](List(1, 2, 3)) should equal(Some(List(1, 2, 3)))
    extractor[List[String]](List("x")) should equal(None)
  }

  sealed trait Foo
  case class Baz(nr: Int, foo: Foo) extends Foo
  case class Bar(name: String) extends Foo
}
