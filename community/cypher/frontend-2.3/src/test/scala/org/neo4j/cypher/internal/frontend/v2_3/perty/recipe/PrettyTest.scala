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
import org.neo4j.cypher.internal.frontend.v2_3.perty.step.AddPretty
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PrettyTest extends CypherFunSuite {

  import Pretty._

  val innerRecipe: RecipeAppender[Any] = "a" :/: "b"
  val innerDoc: ConsDoc = ConsDoc(TextDoc("a"), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"), NilDoc)))

  test("Sane preconditions of the test class") {
    pprintRecipeToDoc(Pretty(innerRecipe)) should equal(innerDoc)
  }

  test("Pretty.nothing") {
    val recipe = Pretty(nothing)

    pprintRecipeToDoc(recipe) should equal(NilDoc)
  }

  test("Pretty.text") {
    val recipe = Pretty(text("a"))

    pprintRecipeToDoc(recipe) should equal(TextDoc("a"))
  }

  test("Pretty.doc") {
    val recipe = Pretty(doc(TextDoc("a")))

    pprintRecipeToDoc(recipe) should equal(TextDoc("a"))
  }

  test("Pretty.literal") {
    val recipe = Pretty(literal(TextDoc("a")))

    recipe.size should equal(1)
    recipe.head.asInstanceOf[AddPretty[Any]].value should equal(DocLiteral(TextDoc("a")))
  }

  test("Pretty.break") {
    val recipe = Pretty(break)

    pprintRecipeToDoc(recipe) should equal(BreakDoc)
  }

  test("Pretty.silentBreak") {
    val recipe = Pretty(silentBreak)

    pprintRecipeToDoc(recipe) should equal(BreakWith(""))
  }

  test("Pretty.breakWith") {
    val recipe = Pretty(breakWith("x"))

    pprintRecipeToDoc(recipe) should equal(BreakWith("x"))
  }

  test("Pretty.breakBefore(nothing)") {
    val recipe = Pretty(breakBefore(nothing))

    pprintRecipeToDoc(recipe) should equal(NilDoc)
  }

  test("Pretty.breakBefore(text)") {
    val recipe = Pretty(breakBefore("x"))

    pprintRecipeToDoc(recipe) should equal(ConsDoc(BreakDoc, ConsDoc(TextDoc("x"), NilDoc)))
  }

  test("Pretty.noBreak") {
    val recipe = Pretty(noBreak)

    pprintRecipeToDoc(recipe) should equal(NoBreak)
  }

  test("Pretty.group") {
    val recipe = Pretty(group(innerRecipe))

    pprintRecipeToDoc(recipe) should equal(GroupDoc(innerDoc))
  }

  test("Pretty.nest") {
    val recipe = Pretty(nest(innerRecipe))

    pprintRecipeToDoc(recipe) should equal(NestDoc(innerDoc))
  }

  test("Pretty.page") {
    val recipe = Pretty(page(innerRecipe))

    pprintRecipeToDoc(recipe) should equal(PageDoc(innerDoc))
  }

  test("Pretty.list") {
    val recipe = Pretty(list(Seq("a", "b")))

    pprintRecipeToDoc(recipe) should equal(ConsDoc(TextDoc("a"), ConsDoc(TextDoc("b"), NilDoc)))
  }

  test("Pretty.breakList") {
    val recipe = Pretty(breakList(Seq("a", "b")))

    pprintRecipeToDoc(recipe) should equal(ConsDoc(TextDoc("a"), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"), NilDoc))))
  }

  test("Pretty.sepList") {
    val recipe = Pretty(sepList(Seq("a", "b")))

    pprintRecipeToDoc(recipe) should equal(
      ConsDoc(TextDoc("a"), ConsDoc(TextDoc(","), ConsDoc(BreakDoc, ConsDoc(TextDoc("b"), NilDoc))))
    )
  }

  test("Pretty.groupedSepList") {
    val recipe = Pretty(groupedSepList(Seq("a", "b")))

    pprintRecipeToDoc(recipe) should equal(
      ConsDoc(GroupDoc(ConsDoc(TextDoc("a"), ConsDoc(TextDoc(","), NilDoc))),
        ConsDoc(BreakDoc, ConsDoc(TextDoc("b"), NilDoc)))
    )
  }

  test("Pretty.parens") {
    val recipe = Pretty(surrounded("(", ")")("neat"))

    pprintRecipeToDoc(recipe) should equal(
      GroupDoc(
        ConsDoc(TextDoc("("),
          ConsDoc(NestDoc(GroupDoc(ConsDoc(BreakDoc, ConsDoc(TextDoc("neat"), NilDoc)))),
            ConsDoc(BreakDoc, ConsDoc(TextDoc(")"), NilDoc)))))
    )
  }

  test("Pretty.brackets") {
    val recipe = Pretty(surrounded("[", "]")("neat"))

    pprintRecipeToDoc(recipe) should equal(
      GroupDoc(
        ConsDoc(TextDoc("["),
          ConsDoc(NestDoc(GroupDoc(ConsDoc(BreakDoc, ConsDoc(TextDoc("neat"), NilDoc)))),
            ConsDoc(BreakDoc, ConsDoc(TextDoc("]"), NilDoc)))))
    )
  }

  test("Pretty.braces") {
    val recipe = Pretty(surrounded("{", "}")("neat"))

    pprintRecipeToDoc(recipe) should equal(
      GroupDoc(
        ConsDoc(TextDoc("{"),
          ConsDoc(NestDoc(GroupDoc(ConsDoc(BreakDoc, ConsDoc(TextDoc("neat"), NilDoc)))),
            ConsDoc(BreakDoc, ConsDoc(TextDoc("}"), NilDoc)))))
    )
  }

  test("Pretty.block") {
    val recipe = Pretty(block("Pretty")("neat"))

    pprintRecipeToDoc(recipe) should equal(
      GroupDoc(ConsDoc(TextDoc("Pretty"), ConsDoc(
        GroupDoc(
          ConsDoc(TextDoc("("),
            ConsDoc(NestDoc(GroupDoc(ConsDoc(BreakWith(""), ConsDoc(TextDoc("neat"), NilDoc)))),
              ConsDoc(BreakWith(""), ConsDoc(TextDoc(")"), NilDoc))))),
        NilDoc
      )))
    )
  }

  def pprintRecipeToDoc(recipe: DocRecipe[Any]): Doc =
    PrintableDocRecipe.evalUsingStrategy().apply(recipe)
}
