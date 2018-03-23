/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Clause}
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class MultipleGraphClausesParsingTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  val fooBarGraph = ast.QualifiedGraphName(List("foo", "bar"))
  val fooDiffGraph = ast.QualifiedGraphName(List("foo", "diff"))

  test("FROM GRAPH foo.bar") {
    yields(ast.FromGraph(fooBarGraph))
  }

  test("CONSTRUCT NEW ()") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    yields(ast.ConstructGraph(creates = List(ast.New(exp.Pattern(patternParts)(pos))(pos))))
  }

  test("CONSTRUCT CLONE a") {
    val item = ast.UnaliasedReturnItem(varFor("a"), "a")(pos)
    val clone = ast.Clone(List(item))(pos)

    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a, b") {
    val item1 = ast.UnaliasedReturnItem(varFor("a"), "a")(pos)
    val item2 = ast.UnaliasedReturnItem(varFor("b"), "b")(pos)
    val clone = ast.Clone(List(item1, item2))(pos)

    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a AS x, b") {
    val item1 = ast.AliasedReturnItem(varFor("a"), varFor("x"))(pos)
    val item2 = ast.UnaliasedReturnItem(varFor("b"), "b")(pos)
    val clone = ast.Clone(List(item1, item2))(pos)

    yields(ast.ConstructGraph(clones = List(clone)))
  }

  test("CONSTRUCT CLONE a CLONE b AS b") {
    val aClone: ast.Clone = ast.Clone(List(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos)
    val bClone: ast.Clone = ast.Clone(List(ast.AliasedReturnItem(varFor("b"), varFor("b"))(pos)))(pos)

    yields(ast.ConstructGraph(clones = List(aClone, bClone)))
  }

  test("CONSTRUCT CLONE x NEW () SET a.prop = 1") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    val clone: ast.Clone = ast.Clone(List(ast.UnaliasedReturnItem(varFor("x"), "x")(pos)))(pos)
    val newClause: ast.New = ast.New(exp.Pattern(patternParts)(pos))(pos)
    val set: ast.SetClause = ast.SetClause(Seq(ast.SetPropertyItem(exp.Property(exp.Variable("a")(pos), exp.PropertyKeyName("prop")(pos))(pos), exp.SignedDecimalIntegerLiteral("1")(pos))(pos)))(pos)

    yields(ast.ConstructGraph(
      clones = List(clone),
      creates = List(newClause),
      sets = List(set))
    )
  }

  test("CONSTRUCT NEW (a) SET a:A") {
    val a = exp.Variable("a")(pos)
    val patternParts = List(exp.EveryPath(exp.NodePattern(Some(a),List(),None)(pos)))
    val newClause: ast.New = ast.New(exp.Pattern(patternParts)(pos))(pos)
    val set: ast.SetClause = ast.SetClause(Seq(ast.SetLabelItem(a, Seq(exp.LabelName("A")(pos)))(pos)))(pos)

    yields(ast.ConstructGraph(
      creates = List(newClause),
      sets = List(set))
    )
  }

  test("CONSTRUCT") {
    yields(ast.ConstructGraph())
  }

  test("CONSTRUCT ON foo.bar") {
    yields(ast.ConstructGraph(on = List(ast.QualifiedGraphName("foo", List("bar")))))
  }

  test("CONSTRUCT ON foo.bar, baz.boz") {
    yields(ast.ConstructGraph(on = List(
      ast.QualifiedGraphName("foo", List("bar")),
      ast.QualifiedGraphName("baz", List("boz"))
    )))
  }

  test("CONSTRUCT SET a:A CREATE (b)") {
    failsToParse
  }

  test("RETURN GRAPH") {
    yields(ast.ReturnGraph(None))
  }

  // TODO: Parsing ambiguity; is it a graph name 'union' or no graph name and a UNION clause?
  ignore("RETURN GRAPH union") {
    yields(ast.ReturnGraph(Some(ast.QualifiedGraphName("union"))))
  }
}
