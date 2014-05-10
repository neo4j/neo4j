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

class LineDocFormatterTest extends CypherFunSuite {

  test("format ConsDoc") {
    LineDocFormatter(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b")))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NilDoc") {
    LineDocFormatter(NilDoc) should equal(Seq.empty)
  }

  test("format TextDoc") {
    LineDocFormatter(TextDoc("text")) should equal(Seq(PrintText("text")))
  }

  test("format BreakDoc") {
    LineDocFormatter(BreakDoc) should equal(Seq(PrintText(" ")))
  }

  test("format BreakWith") {
    LineDocFormatter(BreakWith("-")) should equal(Seq(PrintText("-")))
  }

  test("format GroupDoc") {
    LineDocFormatter(GroupDoc(ConsDoc(TextDoc("a"), TextDoc("b")))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NestDoc") {
    LineDocFormatter(NestWith(10, NilDoc)) should equal(Seq.empty)
  }
}
