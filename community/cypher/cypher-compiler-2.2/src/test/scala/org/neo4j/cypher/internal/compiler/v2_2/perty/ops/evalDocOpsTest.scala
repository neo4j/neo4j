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
package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.perty._

class evalDocOpsTest extends CypherFunSuite {

  // Integration

  test("Builds complex document") {
    val result = evalDocOps(Seq(AddContent("x"), PushGroupFrame, PushNestFrame, AddContent("y"), AddBreak, AddContent("z"), PopFrame, PopFrame))

    result should equal(
      GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(GroupDoc(NestDoc(ConsDoc(TextDoc("y"), ConsDoc(BreakDoc, ConsDoc(TextDoc("z")))))), NilDoc)))
    )
  }

  // Top-Level

  test("Wraps multiple, unwrapped top-level leaves in group") {
    val result = evalDocOps(Seq(AddContent("x"), AddBreak))

    result should equal(GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Builds nil doc for empty seq of doc ops") {
    val result = evalDocOps(Seq())

    result should equal(NilDoc)
  }

  test("Unwraps single top-level value doc in group") {
    val result = evalDocOps(Seq(AddContent("x")))

    result should equal(TextDoc("x"))
  }

  // Groupings

  test("Makes groups containing non-consed doc") {
    val result = evalDocOps(Seq(PushGroupFrame, PushNestFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(GroupDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes groups containing multiples leaves") {
    val result = evalDocOps(Seq(PushGroupFrame, AddContent("x"), AddBreak, PopFrame))

    result should equal(GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building group containing no leaves") {
    evalDocOps(Seq(PushGroupFrame, PopFrame)) should equal(NilDoc)
  }

  test("Unwraps double groupings") {
    val result = evalDocOps(Seq(PushGroupFrame, PushGroupFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  // Pages

  test("Makes pages containing non-consed doc") {
    val result = evalDocOps(Seq(PushPageFrame, PushNestFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(PageDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes pages containing multiples leaves") {
    val result = evalDocOps(Seq(PushPageFrame, AddContent("x"), AddBreak, PopFrame))

    result should equal(PageDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building page containing no leaves") {
    evalDocOps(Seq(PushPageFrame, PopFrame)) should equal(NilDoc)
  }

  test("Unwraps single page in outer page") {
    val result = evalDocOps(Seq(PushPageFrame, PushPageFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(PageDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  // Nesting

  test("Makes nesting groups containing non-consed doc") {
    val result = evalDocOps(Seq(PushNestFrame, PushNestFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(NestDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes nesting groups containing multiples leaves") {
    val result = evalDocOps(Seq(PushNestFrame, AddContent("x"), AddBreak, PopFrame))

    result should equal(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building nesting group containing no leaves") {
    evalDocOps(Seq(PushNestFrame, PopFrame)) should equal(NilDoc)
  }

  test("Does not unwrap double nesting groupings") {
    val result = evalDocOps(Seq(PushNestFrame, PushNestFrame, AddContent("x"), AddBreak, PopFrame, PopFrame))

    result should equal(NestDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }
}
