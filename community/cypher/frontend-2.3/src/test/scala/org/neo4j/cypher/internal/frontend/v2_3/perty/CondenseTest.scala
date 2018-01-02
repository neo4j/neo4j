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
package org.neo4j.cypher.internal.frontend.v2_3.perty

import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintNewLine, PrintText, condense}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CondenseTest extends CypherFunSuite {

  test("Keeps single PrintText (using Seq)") {
    condense(Seq(PrintText("a"))) should equal(Seq(PrintText("a")))
  }

  test("Combines two PrintTexts (using Seq)") {
    condense(Seq(PrintText("a"), PrintText("b"))) should equal(Seq(PrintText("ab")))
  }

  test("Keeps new lines (using Seq)") {
    condense(Seq(PrintText("a"), PrintNewLine(2), PrintText("b"))) should equal(Seq(PrintText("a"), PrintNewLine(2), PrintText("b")))
  }

  test("Keeps single PrintText (using Vector)") {
    condense(Vector(PrintText("a"))) should equal(Vector(PrintText("a")))
  }

  test("Combines two PrintTexts (using Vector)") {
    condense(Vector(PrintText("a"), PrintText("b"))) should equal(Vector(PrintText("ab")))
  }

  test("Keeps new lines (using Vector)") {
    condense(Vector(PrintText("a"), PrintNewLine(2), PrintText("b"))) should equal(Vector(PrintText("a"), PrintNewLine(2), PrintText("b")))
  }

  test("Removes trailing space") {
    condense(Vector(PrintText(""))) should equal(Vector(PrintText("")))
    condense(Vector(PrintText(" "))) should equal(Vector(PrintText("")))
    condense(Vector(PrintText("a "))) should equal(Vector(PrintText("a")))
    condense(Vector(PrintText("a"))) should equal(Vector(PrintText("a")))
    condense(Vector(PrintText(" a b"))) should equal(Vector(PrintText(" a b")))
    condense(Vector(PrintText(" a b  "))) should equal(Vector(PrintText(" a b")))
  }
}
