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
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintNewLine, PrintText}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PageDocFormatterTest extends CypherFunSuite {

  test("format ConsDoc") {
    PageDocFormatter(10)(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NilDoc") {
    PageDocFormatter(10)(NilDoc) should equal(Seq.empty)
  }

  test("format NoBreak") {
    PageDocFormatter(10)(NoBreak) should equal(Seq.empty)
  }

  test("format TextDoc") {
    PageDocFormatter(10)(TextDoc("text")) should equal(Seq(PrintText("text")))
  }

  test("format BreakDoc") {
    PageDocFormatter(10)(BreakDoc) should equal(Seq(PrintText(" ")))
  }

  test("format BreakWith") {
    PageDocFormatter(10)(BreakWith("-")) should equal(Seq(PrintText("-")))
  }

  test("format GroupDoc") {
    PageDocFormatter(10)(GroupDoc(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc)))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NestDoc") {
    PageDocFormatter(10)(NestWith(10, NilDoc)) should equal(Seq.empty)
  }

  test("introduces newlines when group does not fit remaining line") {
    val result = PageDocFormatter(4)(GroupDoc(ConsDoc(TextDoc("hello"), ConsDoc(BreakDoc, ConsDoc(TextDoc("world"), NilDoc)))))
    result should equal(Seq(PrintText("hello"), PrintNewLine(0), PrintText("world")))
  }

  test("honors nesting when introducing newlines") {
    val result = PageDocFormatter(6)(NestWith(2, GroupDoc(ConsDoc(TextDoc("hello"), ConsDoc(BreakDoc, ConsDoc(TextDoc("world"), NilDoc))))))
    result should equal(Seq(PrintText("hello"), PrintNewLine(2), PrintText("world")))
  }

  test("converts breaks to newline in page") {
    val result = PageDocFormatter(100)(PageDoc(ConsDoc(TextDoc("a"), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"), NilDoc)))))
    result should equal(Seq(PrintText("a"), PrintNewLine(0), PrintText("b")))
  }

  test("does not convert breaks to newline in group in page but on outside of it") {
    val aAndB = GroupDoc(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc)))
    val aAndBAndC = ConsDoc(aAndB, ConsDoc(BreakDoc, ConsDoc(TextDoc("c"), NilDoc)))
    val result = PageDocFormatter(100)(PageDoc(aAndBAndC))
    result should equal(Seq(PrintText("a"), PrintText("b"), PrintNewLine(0), PrintText("c")))
  }
}
