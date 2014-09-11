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

import org.scalatest.{FunSuite, Matchers}

import scala.language.implicitConversions

class SimpleDrillTest extends FunSuite with Matchers {

  import Drill._
  import Extractor._

  type TestDrill = Drill[Any, String]

  test("SimpleDrill can be recovered") {
    val drill1: TestDrill = mkDrill {
      inner => pick[Any, String] {
        case (a1, a2) =>
          for (v1 <- inner(a1);
               v2 <- inner(a2))
            yield s"($v1, $v2)"
        case s: String => Some(s)
      }
    }

    val drill2: TestDrill = drill1.recover { pick[Any, String] { case i: Int => Some("-") } }

    drill1(("x", "y")) should equal(Some("(x, y)"))
    drill1((1, "y")) should equal(None)

    drill2(("x", "y")) should equal(Some("(x, y)"))
    drill2((1, "y")) should equal(Some("(-, y)"))
  }
}
