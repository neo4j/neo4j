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
package org.neo4j.cypher.internal.compiler.v2_1.perty

import org.neo4j.cypher.internal.commons.CypherFunSuite

class DocTest extends CypherFunSuite {

  import Doc._

  test("cons(hd, tl) = ConsDocs(hd, tl)") {
    cons(BreakDoc, NilDoc) should equal(ConsDoc(BreakDoc, NilDoc))
  }

  test("empty == NilDoc") {
    Doc.nil should equal(NilDoc)
  }

  test("text(v) = TextDoc(v)") {
    text("text") should equal(TextDoc("text"))
  }

  test("break == BreakDoc") {
    break should equal(BreakDoc)
  }

  test("breakWith(v) == BreakWith(v)") {
    breakWith("a") should equal(BreakWith("a"))
  }

  test("group(...) = GroupDoc(...)") {
    group(ConsDoc(BreakDoc, NilDoc)) should equal(GroupDoc(ConsDoc(BreakDoc, NilDoc)))
  }

  test("nest(...) = NestWith(...)") {
    nest(BreakDoc) should equal(NestDoc(BreakDoc))
  }

  test("nest(i, ...) = NestWith(i, ...)") {
    nest(3, BreakDoc) should equal(NestWith(3, BreakDoc))
  }

  test("list(a :: b :: nil) => cons(a, cons(b))") {
    list(List("a", "b")) should equal(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"))))
  }

  test("sepList(a :: b) => cons(a, cons(',', breakCons(b)))") {
    sepList(List("a", "b")) should equal(ConsDoc(TextDoc("a"), ConsDoc(TextDoc(","), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"))))))
  }

  test("nil :?: a => a") {
    nil :?: text("a") should equal(text("a"))
  }

  test("a :?: nil => a") {
    text("a") :?: nil should equal(text("a"))
  }

  test("a :?: b => b") {
    text("a") :?: text("b") should equal(text("b"))
  }

  test("nil :+: a => a") {
    nil :+: text("a") should equal(text("a"))
  }

  test("a :+: nil => a") {
    text("a") :+: nil should equal(text("a"))
  }

  test("a :+: b => a :/: b") {
    text("a") :+: text("b") should equal(text("a") :/: text("b"))
  }

  test("literal(a) => DocLiteral(a)") {
    literal(text("a")) should equal(DocLiteral(text("a")))
  }

  test("page(a) => PageDoc(a)") {
    page(text("a")) should equal(PageDoc(text("a")))
  }
}
