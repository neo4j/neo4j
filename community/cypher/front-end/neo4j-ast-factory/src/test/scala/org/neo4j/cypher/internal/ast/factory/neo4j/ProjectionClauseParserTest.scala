/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst

class ProjectionClauseParserTest extends ParserSyntaxTreeBase[Cst.Clause, ast.Clause] {

  implicit val javaccRule: JavaccRule[Clause] = JavaccRule.Clause
  implicit val antlrRule: AntlrRule[Cst.Clause] = AntlrRule.Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(
      includeExisting = false,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos)))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(
      includeExisting = true,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos)))
  }

  test("WITH ") {
    failsToParse
  }

  test("RETURN *") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("RETURN 1 AS a") {
    yields(ast.Return(ast.ReturnItems(
      includeExisting = false,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos)))
  }

  test("RETURN *, 1 AS a") {
    yields(ast.Return(ast.ReturnItems(
      includeExisting = true,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos)))
  }

  test("RETURN ") {
    failsToParse
  }

  test("RETURN GRAPH *") {
    failsToParse
  }
}
