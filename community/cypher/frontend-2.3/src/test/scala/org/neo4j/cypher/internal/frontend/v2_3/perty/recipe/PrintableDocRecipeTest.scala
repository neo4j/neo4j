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
package org.neo4j.cypher.internal.frontend.v2_3.perty.recipe

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.step._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PrintableDocRecipeTest extends CypherFunSuite {

  import PrintableDocRecipe.eval

  // Integration

  test("Builds complex document") {
    val result = eval(Seq(AddText("x"), PushGroupFrame, PushNestFrame, AddText("y"), AddBreak, AddText("z"), PopFrame, PopFrame))

    result should equal(
      ConsDoc(TextDoc("x"), ConsDoc(GroupDoc(NestDoc(ConsDoc(TextDoc("y"), ConsDoc(BreakDoc, ConsDoc(TextDoc("z")))))), NilDoc))
    )
  }

  // Top-Level

  test("Builds multiple, unwrapped top-level leaves") {
    val result = eval(Seq(AddText("x"), AddBreak))

    result should equal(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))
  }

  test("Builds nil doc for empty seq of doc ops") {
    val result = eval(Seq())

    result should equal(NilDoc)
  }

  test("Unwraps single top-level value doc in group") {
    val result = eval(Seq(AddText("x")))

    result should equal(TextDoc("x"))
  }

  // Groupings

  test("Makes groups containing non-consed doc") {
    val result = eval(Seq(PushGroupFrame, PushNestFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(GroupDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes groups containing multiples leaves") {
    val result = eval(Seq(PushGroupFrame, AddText("x"), AddBreak, PopFrame))

    result should equal(GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building group containing no leaves") {
    eval(Seq(PushGroupFrame, PopFrame)) should equal(NilDoc)
  }

  test("Unwraps double groupings") {
    val result = eval(Seq(PushGroupFrame, PushGroupFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(GroupDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  // Pages

  test("Makes pages containing non-consed doc") {
    val result = eval(Seq(PushPageFrame, PushNestFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(PageDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes pages containing multiples leaves") {
    val result = eval(Seq(PushPageFrame, AddText("x"), AddBreak, PopFrame))

    result should equal(PageDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building page containing no leaves") {
    eval(Seq(PushPageFrame, PopFrame)) should equal(NilDoc)
  }

  test("Unwraps single page in outer page") {
    val result = eval(Seq(PushPageFrame, PushPageFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(PageDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  // Nesting

  test("Makes nesting groups containing non-consed doc") {
    val result = eval(Seq(PushNestFrame, PushNestFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(NestDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }

  test("Makes nesting groups containing multiples leaves") {
    val result = eval(Seq(PushNestFrame, AddText("x"), AddBreak, PopFrame))

    result should equal(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc))))
  }

  test("Returns nil when building nesting group containing no leaves") {
    eval(Seq(PushNestFrame, PopFrame)) should equal(NilDoc)
  }

  test("Does not unwrap double nesting groupings") {
    val result = eval(Seq(PushNestFrame, PushNestFrame, AddText("x"), AddBreak, PopFrame, PopFrame))

    result should equal(NestDoc(NestDoc(ConsDoc(TextDoc("x"), ConsDoc(BreakDoc, NilDoc)))))
  }
}
