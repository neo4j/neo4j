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
package org.neo4j.cypher.internal.frontend.v2_3.perty.format

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LineFitterTest extends CypherFunSuite {

  test("fits cons") {
    LineFitter.fitsDoc(2, ConsDoc(TextDoc("m"), ConsDoc(TextDoc("e")))) should equal(true)
    LineFitter.fitsDoc(2, ConsDoc(TextDoc("y"), ConsDoc(TextDoc("ou")))) should equal(false)
  }

  test("fits end") {
    LineFitter.fitsDoc(0, NilDoc) should equal(true)
  }

  test("fits text") {
    LineFitter.fitsDoc(2, TextDoc("me")) should equal(true)
    LineFitter.fitsDoc(2, TextDoc("you")) should equal(false)
  }

  test("fits breaks") {
    LineFitter.fitsDoc(1, BreakDoc) should equal(true)
    LineFitter.fitsDoc(0, BreakDoc) should equal(false)
  }

  test("fits breakWith") {
    LineFitter.fitsDoc(2, BreakWith("me")) should equal(true)
    LineFitter.fitsDoc(2, BreakWith("you")) should equal(false)
  }

  test("fits group") {
    LineFitter.fitsDoc(2, GroupDoc(ConsDoc(TextDoc("m"), ConsDoc(TextDoc("e"), NilDoc)))) should equal(true)
    LineFitter.fitsDoc(2, GroupDoc(ConsDoc(TextDoc("y"), ConsDoc(TextDoc("ou"), NilDoc)))) should equal(false)
  }

  test("fits nest") {
    LineFitter.fitsDoc(4, NestWith(2, ConsDoc(TextDoc("m"), ConsDoc(TextDoc("e"), NilDoc)))) should equal(true)
    LineFitter.fitsDoc(4, NestWith(2, ConsDoc(TextDoc("y"), ConsDoc(TextDoc("ou"), NilDoc)))) should equal(true)
  }
}
