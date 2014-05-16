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
package org.neo4j.cypher.internal.compiler.v2_1.pprint

import org.neo4j.cypher.internal.commons.CypherFunSuite

class DocTest extends CypherFunSuite {

  test("cons(hd, tl) = ConsDocs(hd, tl)") {
    Doc.cons(BreakDoc, NilDoc) should equal(ConsDoc(BreakDoc, NilDoc))
  }

  test("empty == NilDoc") {
    Doc.nil should equal(NilDoc)
  }

  test("text(v) = TextDoc(v)") {
    Doc.text("text") should equal(TextDoc("text"))
  }

  test("break == BreakDoc") {
    Doc.breakHere should equal(BreakDoc)
  }

  test("breakWith(v) == BreakWith(v)") {
    Doc.breakWith("a") should equal(BreakWith("a"))
  }

  test("group(...) = GroupDoc(...)") {
    Doc.group(ConsDoc(BreakDoc, NilDoc)) should equal(GroupDoc(ConsDoc(BreakDoc, NilDoc)))
  }

  test("nest(...) = NestWith(...)") {
    Doc.nest(BreakDoc) should equal(NestDoc(BreakDoc))
  }

  test("nest(i, ...) = NestWith(i, ...)") {
    Doc.nest(3, BreakDoc) should equal(NestWith(3, BreakDoc))
  }

  test("breakCons(hd, tl) = cons(hd, cons(breakHere, tl))") {
    Doc.breakCons(Doc.text("a"), Doc.text("b")) should equal(ConsDoc(TextDoc("a"), ConsDoc(BreakDoc, TextDoc("b"))))
  }

  test("list(a :: b :: nil) => cons(a, cons(b))") {
    Doc.list(List("a", "b")) should equal(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"))))
  }

  test("sepList(a :: b) => cons(a, cons(',', breakCons(b)))") {
    Doc.sepList(List("a", "b")) should equal(ConsDoc(TextDoc("a"), ConsDoc(TextDoc(","), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"))))))
  }
}
