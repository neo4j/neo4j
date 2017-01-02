/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.parser

import org.neo4j.cypher.internal.frontend.v3_2.{DummyPosition, ast}

class FunctionInvocationParserTest
  extends ParserAstTest[ast.FunctionInvocation]
    with Expressions
    with Literals
    with Base
    with ProcedureCalls  {

  implicit val parser = FunctionInvocation

  test("foo()") {
    yields(ast.FunctionInvocation(ast.Namespace()(pos), ast.FunctionName("foo")(pos), distinct = false, Vector.empty))
  }

  test("foo('test', 1 + 2)") {
    yields(ast.FunctionInvocation(ast.Namespace()(pos), ast.FunctionName("foo")(pos), distinct = false, Vector(
      ast.StringLiteral("test")(pos),
      ast.Add(
        ast.SignedDecimalIntegerLiteral("1")(pos),
        ast.SignedDecimalIntegerLiteral("2")(pos))(pos)
    )))
  }
  test("my.namespace.foo()") {
    yields(ast.FunctionInvocation(ast.Namespace(List("my", "namespace"))(pos), ast.FunctionName("foo")(pos), distinct = false, Vector.empty))
  }

  test("my.namespace.foo('test', 1 + 2)") {
    yields(ast.FunctionInvocation(ast.Namespace(List("my", "namespace"))(pos), ast.FunctionName("foo")(pos), distinct = false, Vector(
      ast.StringLiteral("test")(pos),
      ast.Add(
        ast.SignedDecimalIntegerLiteral("1")(pos),
        ast.SignedDecimalIntegerLiteral("2")(pos))(pos)
    )))
  }


  private val pos = DummyPosition(-1)

  implicit class StringToVariable(string: String) {
    def asVar = id(string)(pos)
  }
}
