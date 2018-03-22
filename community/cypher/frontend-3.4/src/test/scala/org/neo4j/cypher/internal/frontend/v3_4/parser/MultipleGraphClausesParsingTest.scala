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

  test("WITH * WHERE a ~ b") {
    val where = ast.Where(exp.Equivalent(varFor("a"), varFor("b"))(pos))(pos)
    yields(ast.With(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), None, None, None, Some(where)))
  }

  test("USE GRAPH foo.bar") {
    yields(ast.UseGraph(fooBarGraph))
  }

  test("CONSTRUCT { CREATE () }") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    yields(ast.ConstructGraph(creates = List(ast.Create(exp.Pattern(patternParts)(pos))(pos))))
  }

  test("CONSTRUCT { MERGE () CREATE () SET a.prop = 1 }") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    val merge: ast.Merge = ast.Merge(exp.Pattern(patternParts)(pos), Seq.empty)(pos)
    val create: ast.Create = ast.Create(exp.Pattern(patternParts)(pos))(pos)
    val set: ast.SetClause = ast.SetClause(Seq(ast.SetPropertyItem(exp.Property(exp.Variable("a")(pos), exp.PropertyKeyName("prop")(pos))(pos), exp.SignedDecimalIntegerLiteral("1")(pos))(pos)))(pos)

    yields(ast.ConstructGraph(
      merges = List(merge),
      creates = List(create),
      sets = List(set))
    )
  }

  test("CONSTRUCT { CREATE (a) SET a:A }") {
    val a = exp.Variable("a")(pos)
    val patternParts = List(exp.EveryPath(exp.NodePattern(Some(a),List(),None)(pos)))
    val create: ast.Create = ast.Create(exp.Pattern(patternParts)(pos))(pos)
    val set: ast.SetClause = ast.SetClause(Seq(ast.SetLabelItem(a, Seq(exp.LabelName("A")(pos)))(pos)))(pos)

    yields(ast.ConstructGraph(
      creates = List(create),
      sets = List(set))
    )
  }

  test("CONSTRUCT { MERGE (a) MERGE (b) SET a:A }") {
    val a = exp.Variable("a")(pos)
    val aPattern = List(exp.EveryPath(exp.NodePattern(Some(a),List(),None)(pos)))
    val aMerge: ast.Merge = ast.Merge(exp.Pattern(aPattern)(pos), Seq.empty)(pos)
    val b = exp.Variable("b")(pos)
    val bPattern = List(exp.EveryPath(exp.NodePattern(Some(b),List(),None)(pos)))
    val bMerge: ast.Merge = ast.Merge(exp.Pattern(bPattern)(pos), Seq.empty)(pos)
    val set: ast.SetClause = ast.SetClause(Seq(ast.SetLabelItem(a, Seq(exp.LabelName("A")(pos)))(pos)))(pos)

    yields(ast.ConstructGraph(
      merges = List(aMerge, bMerge),
      sets = List(set))
    )
  }

  test("CONSTRUCT {}") {
    yields(ast.ConstructGraph())
  }

  test("CONSTRUCT { SET a:A CREATE (b) }") {
    failsToParse
  }

  test("CREATE GRAPH foo.bar") {
    yields(ast.CreateGraph(fooBarGraph))
  }

  test("COPY GRAPH foo.bar TO foo.diff") {
    yields(ast.CopyGraph(fooBarGraph, fooDiffGraph))
  }

  test("RENAME GRAPH foo.bar TO foo.diff") {
    yields(ast.RenameGraph(fooBarGraph, fooDiffGraph))
  }

  test("TRUNCATE GRAPH foo.bar") {
    yields(ast.TruncateGraph(fooBarGraph))
  }

  test("DELETE GRAPH foo.bar") {
    yields(ast.DeleteGraph(fooBarGraph))
  }

  test("RETURN GRAPH") {
    yields(ast.ReturnGraph(None))
  }

  test("RETURN GRAPH foo.bar") {
    yields(ast.ReturnGraph(Some(fooBarGraph)))
  }

  private val nodePattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, List(), None)(pos))))(pos)

  private val complexPattern = exp.Pattern(List(
    exp.NamedPatternPart(varFor("p"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos),
    exp.NamedPatternPart(varFor("q"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos)
  ))(pos)
}
