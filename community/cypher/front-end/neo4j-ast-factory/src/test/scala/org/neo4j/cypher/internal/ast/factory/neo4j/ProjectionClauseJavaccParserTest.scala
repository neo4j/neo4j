/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast

class ProjectionClauseJavaccParserTest extends JavaccParserAstTestBase[ast.Clause] {

  implicit val parser: JavaccRule[ast.Clause] = JavaccRule.Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos, isAutoAliased = false)))(pos)))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos, isAutoAliased = false)))(pos)))
  }

  test("WITH ") {
    failsToParse
  }

  test("RETURN *") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos)))
  }

  test("RETURN 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos, isAutoAliased = false)))(pos)))
  }

  test("RETURN *, 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos, isAutoAliased = false)))(pos)))
  }

  test("RETURN ") {
    failsToParse
  }

  test("RETURN GRAPH *") {
    failsToParse
  }
}
