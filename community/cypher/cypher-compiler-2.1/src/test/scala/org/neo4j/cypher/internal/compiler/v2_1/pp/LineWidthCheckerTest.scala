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
package org.neo4j.cypher.internal.compiler.v2_1.pp

import org.neo4j.cypher.internal.commons.CypherFunSuite

class LineWidthCheckerTest extends CypherFunSuite {

  import Doc._

  test("fits cons") {
    LineWidthChecker.fitsDoc(2, cons(text("m"), cons(text("e")))) should equal(true)
    LineWidthChecker.fitsDoc(2, cons(text("y"), cons(text("ou")))) should equal(false)
  }

  test("fits end") {
    LineWidthChecker.fitsDoc(0, end) should equal(true)
  }

  test("fits text") {
    LineWidthChecker.fitsDoc(2, text("me")) should equal(true)
    LineWidthChecker.fitsDoc(2, text("you")) should equal(false)
  }

  test("fits breaks") {
    LineWidthChecker.fitsDoc(1, break) should equal(true)
    LineWidthChecker.fitsDoc(0, break) should equal(false)
  }

  test("fits breakWith") {
    LineWidthChecker.fitsDoc(2, breakWith("me")) should equal(true)
    LineWidthChecker.fitsDoc(2, breakWith("you")) should equal(false)
  }

  test("fits group") {
    LineWidthChecker.fitsDoc(2, group(cons(text("m"), cons(text("e"))))) should equal(true)
    LineWidthChecker.fitsDoc(2, group(cons(text("y"), cons(text("ou"))))) should equal(false)
  }

  test("fits nest") {
    LineWidthChecker.fitsDoc(4, nest(2, cons(text("m"), cons(text("e"))))) should equal(true)
    LineWidthChecker.fitsDoc(4, nest(2, cons(text("y"), cons(text("ou"))))) should equal(true)
  }
}
