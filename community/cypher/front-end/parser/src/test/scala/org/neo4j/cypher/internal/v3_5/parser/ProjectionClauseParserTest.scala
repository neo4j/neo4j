/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.parser

import org.neo4j.cypher.internal.v3_5.ast
import org.neo4j.cypher.internal.v3_5.expressions
import org.neo4j.cypher.internal.v3_5.ast.{AstConstructionTestSupport, Clause}
import org.parboiled.scala._

class ProjectionClauseParserTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos)))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos)))
  }

  test("WITH ") {
    failsToParse
  }

  test("WITH * WHERE a ~ b") {
    val where = ast.Where(expressions.Equivalent(varFor("a"), varFor("b"))(pos))(pos)
    yields(ast.With(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), None, None, None, Some(where)))
  }

  test("RETURN *") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("RETURN 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos)))
  }

  test("RETURN *, 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos)))
  }

  // TODO: Test valid RETURN GRAPH

  test("RETURN ") {
    failsToParse
  }

  test("RETURN GRAPH *") {
    failsToParse
  }

}
