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
package org.neo4j.cypher.internal.frontend.v2_3.perty.gen

import org.neo4j.cypher.internal.frontend.v2_3.perty._

class DocStructureDocGenTest  extends DocHandlerTestSuite[Doc] {

  val docGen = docStructureDocGen

  test("nil => \"ø\"") {
    pprintToString(NilDoc) should equal("ø")
  }

  test("break => \"·\"") {
    pprintToString(BreakDoc) should equal("·")
  }

  test("breakWith(...) => \"·...·\"") {
    pprintToString(BreakWith("...")) should equal("·...·")
  }

  test("text(...) => \"...\"") {
    pprintToString[Doc](TextDoc("...")) should equal("\"...\"")
  }

  test("text(\"a\")·text(\"b\") => \"a\" ⸬ \"b\" ⸬ ø") {
    pprintToString(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc))) should equal("\"a\" ⸬ \"b\" ⸬ ø")
  }

  test("group(text(\"a\")) => [\"a\"]") {
    pprintToString(GroupDoc(TextDoc("a"))) should equal("[\"a\"]")
  }

  test("nest(3, text(\"a\")) => (3)<\"a\">") {
    pprintToString(NestWith(3, TextDoc("a"))) should equal("(3)<\"a\">")
  }

  test("nest(text(\"a\")) => (3)<\"a\">") {
    pprintToString(NestDoc(TextDoc("a"))) should equal("<\"a\">")
  }

  test("page(group(\"a\"))) => (|[\"a\"]|)") {
    pprintToString(PageDoc(GroupDoc(TextDoc("a")))) should equal("(|[\"a\"]|)")
  }

  test("group(page(\"a\"))) => [(|\"a\"|)]") {
    pprintToString(GroupDoc(PageDoc(TextDoc("a")))) should equal("[(|\"a\"|)]")
  }
}
