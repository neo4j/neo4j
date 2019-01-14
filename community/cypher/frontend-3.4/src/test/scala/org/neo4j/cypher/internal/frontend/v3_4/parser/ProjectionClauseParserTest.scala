/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Clause, GraphReturnItems, PassAllGraphReturnItems}
import org.parboiled.scala.Rule1

class ProjectionClauseParserTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH ") {
    failsToParse
  }

  test("RETURN *") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), None))
  }

  test("RETURN 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("RETURN *, 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("RETURN ") {
    failsToParse
  }

}
