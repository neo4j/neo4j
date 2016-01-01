/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.perty.impl

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.perty.Doc

class LineFitterTest extends CypherFunSuite {

  import Doc._

  test("fits cons") {
    LineFitter.fitsDoc(2, cons(text("m"), cons(text("e")))) should equal(true)
    LineFitter.fitsDoc(2, cons(text("y"), cons(text("ou")))) should equal(false)
  }

  test("fits end") {
    LineFitter.fitsDoc(0, nil) should equal(true)
  }

  test("fits text") {
    LineFitter.fitsDoc(2, text("me")) should equal(true)
    LineFitter.fitsDoc(2, text("you")) should equal(false)
  }

  test("fits breaks") {
    LineFitter.fitsDoc(1, break) should equal(true)
    LineFitter.fitsDoc(0, break) should equal(false)
  }

  test("fits breakWith") {
    LineFitter.fitsDoc(2, breakWith("me")) should equal(true)
    LineFitter.fitsDoc(2, breakWith("you")) should equal(false)
  }

  test("fits group") {
    LineFitter.fitsDoc(2, group(cons(text("m"), cons(text("e"))))) should equal(true)
    LineFitter.fitsDoc(2, group(cons(text("y"), cons(text("ou"))))) should equal(false)
  }

  test("fits nest") {
    LineFitter.fitsDoc(4, nest(2, cons(text("m"), cons(text("e"))))) should equal(true)
    LineFitter.fitsDoc(4, nest(2, cons(text("y"), cons(text("ou"))))) should equal(true)
  }
}
