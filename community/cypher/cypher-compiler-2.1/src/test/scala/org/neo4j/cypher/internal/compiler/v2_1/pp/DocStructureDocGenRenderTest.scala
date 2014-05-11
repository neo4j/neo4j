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
import org.neo4j.cypher.internal.compiler.v2_1.pp.docgen.docStructureDocGen

class DocStructureDocGenRenderTest extends CypherFunSuite {

  import Doc._

  test("end => \"ø\"") {
    render(end) should equal("ø")
  }

  test("break => \"_\"") {
    render(breakHere) should equal("_")
  }

  test("breakWith(...) => \"_..._\"") {
    render(breakWith("...")) should equal("_..._")
  }

  test("text(...) => \"...\"") {
    render(text("...")) should equal("\"...\"")
  }

  test("text(\"a\")·text(\"b\") => \"a\"·\"b\"·ø") {
    render(cons(text("a"), cons(text("b")))) should equal("\"a\"·\"b\"·ø")
  }

  test("group(text(\"a\")) => [\"a\"]") {
    render(group(text("a"))) should equal("[\"a\"]")
  }

  test("nest(3, text(\"a\")) => (3)<\"a\">") {
    render(nest(3, text("a"))) should equal("(3)<\"a\">")
  }

  test("nest(text(\"a\")) => (3)<\"a\">") {
    render(nest(text("a"))) should equal("<\"a\">")
  }

  private def render(doc: Doc) = printString(LineDocFormatter(docStructureDocGen(doc)))
}
