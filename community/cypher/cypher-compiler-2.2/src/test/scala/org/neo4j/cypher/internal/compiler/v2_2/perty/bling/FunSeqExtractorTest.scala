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

import scala.language.implicitConversions

import org.scalatest.{FunSuite, Matchers}

import scala.reflect.runtime.universe.TypeTag

class FunSeqExtractorTest extends FunSuite with Matchers {
  type TestDrill = Drill[Any, String]
  type TestExtractor = FunSeqExtractor[Any, String]

  import Extractor._

  test("Constructs empty extractor") {
    val extractor = FunSeqExtractorFactory.empty[Any, String]
    val result = extractor.fix(toStringExtractor)("String")

    result should equal(None)
  }

  test("Constructs and applies extractor from single drill to matching value") {
    val drill: TestDrill = inner => pick { case i: Int => Some("X") }
    val extractor = FunSeqExtractorFactory.fromSingle[Any, String](drill)
    val result = extractor.fix(toStringExtractor)(1)

    result should equal(Some("X"))
  }

  test("Constructs and applies extractor from single drill to non-matching value") {
    val drill: TestDrill = inner => pick { case i: Int => Some("X") }
    val extractor = FunSeqExtractorFactory.fromSingle[Any, String](drill)
    val result = extractor.fix(toStringExtractor)(true)

    result should equal(None)
  }

  test("Constructs and applies extractor from two drills to non-matching value") {
    val drill1: TestDrill = inner => pick { case i: Int => Some("X") }
    val drill2: TestDrill = inner => pick { case b: Boolean => Some("Y") }
    val extractor = FunSeqExtractorFactory[Any, String](drill1, drill2)
    val result = extractor.fix(toStringExtractor)(3.14f)

    result should equal(None)
  }

  test("Constructs and applies extractor from two drills to matching value for first drill") {
    val drill1: TestDrill = inner => pick { case i: Int => Some("X") }
    val drill2: TestDrill = inner => pick { case b: Boolean => Some("Y") }
    val extractor = FunSeqExtractorFactory[Any, String](drill1, drill2)
    val result = extractor.fix(toStringExtractor)(1)

    result should equal(Some("X"))
  }

  test("Constructs and applies extractor from two drills to matching value for second drill") {
    val drill1: TestDrill = inner => pick { case i: Int => Some("X") }
    val drill2: TestDrill = inner => pick { case b: Boolean => Some("Y") }
    val extractor = FunSeqExtractorFactory[Any, String](drill1, drill2)
    val result = extractor.fix(toStringExtractor)[Boolean](true)

    result should equal(Some("Y"))
  }

  test("Earlier drill shadows later drill") {
    val drill1: TestDrill = inner => pick { case b: Boolean if b => Some("X") }
    val drill2: TestDrill = inner => pick { case b: Boolean => Some("Y") }
    val extractor = FunSeqExtractorFactory[Any, String](drill1, drill2)
    val result1 = extractor.fix(toStringExtractor)(true)
    val result2 = extractor.fix(toStringExtractor)(false)

    (result1, result2) should equal(Some("X"), Some("Y"))
  }

  test("FunSeqExtractors compose with other FunSeqExtractors") {
    val drill1: TestDrill = inner => pick { case i: Int => Some("X") }
    val drill2: TestDrill = inner => pick { case i: Int => Some("Y") }
    val drill3: TestDrill = inner => pick { case i: Int => Some("Z") }

    val extractor1 = FunSeqExtractorFactory(drill1)
    val extractor2 = FunSeqExtractorFactory(drill2, drill3)

    val composedExtractor = extractor1 ++ extractor2

    composedExtractor.drills should equal(Seq(drill1, drill2, drill3))
  }

  test("FunSeqExtractor can be mapped with mapInput") {
    val drill: TestDrill = inner => pick { case x => inner(">" + x.toString) }
    val extractor = FunSeqExtractorFactory(drill).mapInput( extract { (x: Any) => Some(x.toString.length + "<") } )
    val result = extractor.fix(toStringExtractor)(123)

    result should equal(Some(">3<"))
  }

  test("FunSeqExtractor extracts over children") {
    val drill1: TestDrill = inner => pick {
      case Some(X(i, opt)) => Some(s"X(${inner(i).get}, ${inner(opt).get})")
    }
    val drill2: TestDrill = inner => pick { case x => Some(s"!$x") }
    val extractor = FunSeqExtractorFactory(drill1, drill2)

    val result = extractor[Option[X]](Some(X(1, Some(X(2, None)))))

    result should equal(Some("X(!1, X(!2, !None))"))
  }

  test("FunSeqExtractor can be used with a fallback") {
    val handler = pick[Any, String] { case _ => Some("X") }
    val drill = handler.recover[(Int, String)] { inner => pick { case (1, b) => inner(b) } }
    val extractor = FunSeqExtractorFactory(drill)

    val result = extractor((1, "-"))

    result should equal(Some("X"))
  }

  case object toStringExtractor extends SimpleExtractor[Any, String] {
    def apply[X <: Any : TypeTag](v: X) = Some(v.toString)
  }

  case class X(i: Int, x: Option[X])
}
