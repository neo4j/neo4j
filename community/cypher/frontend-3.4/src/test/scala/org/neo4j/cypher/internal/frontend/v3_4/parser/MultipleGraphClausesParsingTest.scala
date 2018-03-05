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
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Clause}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class MultipleGraphClausesParsingTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("USE GRAPH foo.bar") {
    yields(ast.UseGraph(ast.QualifiedGraphName(List("foo", "bar"))))
  }

  test("CONSTRUCT GRAPH { CREATE () }") {
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    yields(ast.ConstructGraph(None, ast.Create(exp.Pattern(patternParts)(pos))(pos)))
  }

  test("CONSTRUCT GRAPH foo.bar { CREATE () }") {
    val qgn = ast.QualifiedGraphName(List("foo", "bar"))
    val patternParts = List(exp.EveryPath(exp.NodePattern(None,List(),None)(pos)))
    yields(ast.ConstructGraph(Some(qgn), ast.Create(exp.Pattern(patternParts)(pos))(pos)))
  }

  test("CREATE GRAPH foo.bar") {
    failsToParse
  }

  test("COPY GRAPH foo.bar TO foo.diff") {
    failsToParse
  }

  test("RENAME GRAPH foo.bar TO foo.diff") {
    failsToParse
  }

  test("TRUNCATE GRAPH foo.bar") {
    failsToParse
  }

  test("DELETE GRAPH foo.bar") {
    failsToParse
  }

  private val nodePattern = exp.Pattern(List(exp.EveryPath(exp.NodePattern(None, List(), None)(pos))))(pos)

  private val complexPattern = exp.Pattern(List(
    exp.NamedPatternPart(varFor("p"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos),
    exp.NamedPatternPart(varFor("q"), exp.EveryPath(exp.NodePattern(None, List(), None)(pos)))(pos)
  ))(pos)
}
