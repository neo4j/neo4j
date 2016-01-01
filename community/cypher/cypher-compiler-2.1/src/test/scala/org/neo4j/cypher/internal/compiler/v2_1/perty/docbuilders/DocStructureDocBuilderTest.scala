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
package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.perty._

class DocStructureDocBuilderTest extends DocBuilderTestSuite[Doc] {

  import Doc._

  val docBuilder = docStructureDocBuilder

  test("end => \"ø\"") {
    format(nil) should equal("ø")
  }

  test("break => \"_\"") {
    format(break) should equal("_")
  }

  test("breakWith(...) => \"_..._\"") {
    format(breakWith("...")) should equal("_..._")
  }

  test("text(...) => \"...\"") {
    format("...") should equal("\"...\"")
  }

  test("text(\"a\")·text(\"b\") => \"a\"·\"b\"·ø") {
    format("a" :: "b" :: nil) should equal("\"a\"·\"b\"·ø")
  }

  test("group(text(\"a\")) => [\"a\"]") {
    format(group("a")) should equal("[\"a\"]")
  }

  test("nest(3, text(\"a\")) => (3)<\"a\">") {
    format(nest(3, "a")) should equal("(3)<\"a\">")
  }

  test("nest(text(\"a\")) => (3)<\"a\">") {
    format(nest("a")) should equal("<\"a\">")
  }

  test("page(group(\"a\"))) => (|[\"a\"]|)") {
    format(page(group("a"))) should equal("(|[\"a\"]|)")
  }

  test("group(page(\"a\"))) => [(|\"a\"|)]") {
    format(group(page("a"))) should equal("[(|\"a\"|)]")
  }
}
