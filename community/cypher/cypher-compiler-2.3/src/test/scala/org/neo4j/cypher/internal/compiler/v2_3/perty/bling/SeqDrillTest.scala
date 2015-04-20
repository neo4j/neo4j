/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
///**
// * Copyright (c) 2002-2015 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.compiler.v2_2.perty.bling
//
//import org.scalatest.{FunSuite, Matchers}
//
//import scala.language.implicitConversions
//
//class SeqDrillTest extends FunSuite with Matchers {
//  type TestDrill = Drill[Any, String]
//  type TestExtractor = SeqDrill[Any, String]
//
//  import Drill._
//  import Extractor._
//
//  test("Constructs empty extractor") {
//    val extractor = SeqDrill.empty[Any, String]
//    val result = extractor.specialize(toStringExtractor)("String")
//
//    result should equal(None)
//  }
//
//  test("Constructs and applies extractor from single drill to matching value") {
//    val drill: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val extractor = SeqDrill.fromSingle[Any, String](drill)
//    val result = extractor.specialize(toStringExtractor)(1)
//
//    result should equal(Some("X"))
//  }
//
//  test("Constructs and applies extractor from single drill to non-matching value") {
//    val drill: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val extractor = SeqDrill.fromSingle[Any, String](drill)
//    val result = extractor.specialize(toStringExtractor)(true)
//
//    result should equal(None)
//  }
//
//  test("Constructs and applies extractor from two drills to non-matching value") {
//    val drill1: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val drill2: TestDrill = mkDrill { inner => pick { case b: Boolean => Some("Y") } }
//    val extractor = SeqDrill[Any, String](drill1, drill2)
//    val result = extractor.specialize(toStringExtractor)(3.14f)
//
//    result should equal(None)
//  }
//
//  test("Constructs and applies extractor from two drills to matching value for first drill") {
//    val drill1: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val drill2: TestDrill = mkDrill { inner => pick { case b: Boolean => Some("Y") } }
//    val extractor = SeqDrill[Any, String](drill1, drill2)
//    val result = extractor.specialize(toStringExtractor)(1)
//
//    result should equal(Some("X"))
//  }
//
//  test("Constructs and applies extractor from two drills to matching value for second drill") {
//    val drill1: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val drill2: TestDrill = mkDrill { inner => pick { case b: Boolean => Some("Y") } }
//    val extractor = SeqDrill[Any, String](drill1, drill2)
//    val result = extractor.specialize(toStringExtractor)[Boolean](true)
//
//    result should equal(Some("Y"))
//  }
//
//  test("Earlier drill shadows later drill") {
//    val drill1: TestDrill = mkDrill { inner => pick { case b: Boolean if b => Some("X") } }
//    val drill2: TestDrill = mkDrill { inner => pick { case b: Boolean => Some("Y") } }
//    val extractor = SeqDrill[Any, String](drill1, drill2)
//    val result1 = extractor.specialize(toStringExtractor)(true)
//    val result2 = extractor.specialize(toStringExtractor)(false)
//
//    (result1, result2) should equal(Some("X"), Some("Y"))
//  }
//
//  test("FunSeqDrills compose with other FunSeqDrills") {
//    val drill1: TestDrill = mkDrill { inner => pick { case i: Int => Some("X") } }
//    val drill2: TestDrill = mkDrill { inner => pick { case i: Int => Some("Y") } }
//    val drill3: TestDrill = mkDrill { inner => pick { case i: Int => Some("Z") } }
//
//    val extractor1 = SeqDrill(drill1)
//    val extractor2 = SeqDrill(drill2, drill3)
//
//    val composedExtractor = extractor1 ++ extractor2
//
//    composedExtractor.drills should equal(Seq(drill1, drill2, drill3))
//  }
//
//  test("FunSeqDrill can be mapped with mapInput") {
//    val drill: TestDrill = mkDrill { inner => pick { case x => inner(">" + x.toString) } }
//    val extractor = SeqDrill(drill).mapInput( extract { (x: Any) => Some(x.toString.length + "<") } )
//    val result = extractor.specialize(toStringExtractor)(123)
//
//    result should equal(Some(">3<"))
//  }
//
//  test("FunSeqDrill extracts over children") {
//    val drill1: TestDrill = mkDrill { inner => pick {
//      case Some(X(i, opt)) => Some(s"X(${inner(i).get}, ${inner(opt).get})")
//    } }
//    val drill2: TestDrill = mkDrill { inner => pick { case x => Some(s"!$x") } }
//    val extractor = SeqDrill(drill1, drill2)
//
//    val result = extractor[Option[X]](Some(X(1, Some(X(2, None)))))
//
//    result should equal(Some("X(!1, X(!2, !None))"))
//  }
//
//  test("SeqDrill can be recovered") {
//    val drill1: TestDrill = mkDrill {
//      inner => pick[Any, String] {
//        case (a1, a2) =>
//          for (v1 <- inner(a1);
//               v2 <- inner(a2))
//          yield s"($v1, $v2)"
//        case s: String => Some(s)
//      }
//    }
//
//    val drill2: TestDrill = drill1.recover { pick[Any, String] { case i: Int => Some("-") } }
//
//    SeqDrill(drill1).apply(("x", "y")) should equal(Some("(x, y)"))
//    SeqDrill(drill1).apply((1, "y")) should equal(None)
//
//    SeqDrill(drill2).apply(("x", "y")) should equal(Some("(x, y)"))
//    SeqDrill(drill2).apply((1, "y")) should equal(Some("(-, y)"))
//  }
//
//
//  case class X(i: Int, x: Option[X])
//}
