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
import org.neo4j.cypher.internal.compiler.v2_1.perty.{PrintNewLine, PrintText, Doc}

class PageDocFormatterTest extends CypherFunSuite {

  import Doc._

  test("format ConsDoc") {
    PageDocFormatter(10)(cons("a", cons("b"))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NilDoc") {
    PageDocFormatter(10)(nil) should equal(Seq.empty)
  }

  test("format TextDoc") {
    PageDocFormatter(10)("text") should equal(Seq(PrintText("text")))
  }

  test("format BreakDoc") {
    PageDocFormatter(10)(break) should equal(Seq(PrintText(" ")))
  }

  test("format BreakWith") {
    PageDocFormatter(10)(breakWith("-")) should equal(Seq(PrintText("-")))
  }

  test("format GroupDoc") {
    PageDocFormatter(10)(group(cons("a", "b"))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NestDoc") {
    PageDocFormatter(10)(nest(10, nil)) should equal(Seq.empty)
  }

  test("introduces newlines when group does not fit remaining line") {
    val result = PageDocFormatter(4)(group("hello" :/: "world"))
    result should equal(Seq(PrintText("hello"), PrintNewLine(0), PrintText("world")))
  }

  test("honors nesting when introducing newlines") {
    val result = PageDocFormatter(6)(nest(2, group("hello" :/: "world")))
    result should equal(Seq(PrintText("hello"), PrintNewLine(2), PrintText("world")))
  }

  test("converts breaks to newline in page") {
    val result = PageDocFormatter(100)(page("a" :/: "b"))
    result should equal(Seq(PrintText("a"), PrintNewLine(0), PrintText("b")))
  }

  test("does not convert breaks to newline in group in page but on outside of it") {
    val result = PageDocFormatter(100)(page(group("a" :: "b") :/: "c"))
    result should equal(Seq(PrintText("a"), PrintText("b"), PrintNewLine(0), PrintText("c")))
  }
}
