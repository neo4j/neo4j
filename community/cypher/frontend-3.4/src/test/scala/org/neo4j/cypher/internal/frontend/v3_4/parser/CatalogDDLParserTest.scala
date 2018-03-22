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
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, ReturnGraph}
import org.parboiled.scala.Rule1

class CatalogDDLParserTest
  extends ParserAstTest[ast.Statement] with Statement with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Statement] = Statement

  private val returnGraph: ReturnGraph = ast.ReturnGraph(None)(pos)

  test("CREATE GRAPH foo.bar { RETURN GRAPH }") {
    val query = ast.SingleQuery(Seq(returnGraph))(pos)
    val graphName = ast.QualifiedGraphName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, query))
  }

  test("CREATE GRAPH foo.bar { USE GRAPH foo RETURN GRAPH UNION ALL USE GRAPH bar RETURN GRAPH }") {
    val useGraph1 = ast.UseGraph(ast.QualifiedGraphName("foo"))(pos)
    val useGraph2 = ast.UseGraph(ast.QualifiedGraphName("bar"))(pos)
    val lhs = ast.SingleQuery(Seq(useGraph1, returnGraph))(pos)
    val rhs = ast.SingleQuery(Seq(useGraph2, returnGraph))(pos)
    val union = ast.UnionAll(lhs, rhs)(pos)
    val graphName = ast.QualifiedGraphName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, union))
  }

  test("CREATE GRAPH foo.bar { USE GRAPH foo RETURN GRAPH UNION USE GRAPH bar RETURN GRAPH }") {
    val useGraph1 = ast.UseGraph(ast.QualifiedGraphName("foo"))(pos)
    val useGraph2 = ast.UseGraph(ast.QualifiedGraphName("bar"))(pos)
    val lhs = ast.SingleQuery(Seq(useGraph1, returnGraph))(pos)
    val rhs = ast.SingleQuery(Seq(useGraph2, returnGraph))(pos)
    val union = ast.UnionDistinct(lhs, rhs)(pos)
    val graphName = ast.QualifiedGraphName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, union))
  }

  // missing graph name
  test("CREATE GRAPH { RETURN GRAPH }") {
    failsToParse
  }

  // missing graph name
  test("DELETE GRAPH union") {
    val graphName = ast.QualifiedGraphName("union")

    yields(ast.DeleteGraph(graphName))
  }

  // missing graph name; doesn't fail because it's a valid query if GRAPH is a variable
  ignore("DELETE GRAPH") {
    failsToParse
  }
}
