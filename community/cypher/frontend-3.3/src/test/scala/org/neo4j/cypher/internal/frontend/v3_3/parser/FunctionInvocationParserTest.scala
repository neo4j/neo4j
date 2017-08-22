/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast

class FunctionInvocationParserTest
    extends ParserAstTest[ast.FunctionInvocation]
    with Expressions
    with Literals
    with Base
    with ProcedureCalls {

  implicit val parser = FunctionInvocation

  test("foo()") {
    yields(ast.FunctionInvocation(ast.Namespace()(pos), ast.FunctionName("foo")(pos), distinct = false, Vector.empty))
  }

  test("foo('test', 1 + 2)") {
    yields(
      ast.FunctionInvocation(
        ast.Namespace()(pos),
        ast.FunctionName("foo")(pos),
        distinct = false,
        Vector(
          ast.StringLiteral("test")(pos),
          ast.Add(ast.SignedDecimalIntegerLiteral("1")(pos), ast.SignedDecimalIntegerLiteral("2")(pos))(pos)
        )
      ))
  }
  test("my.namespace.foo()") {
    yields(
      ast.FunctionInvocation(ast.Namespace(List("my", "namespace"))(pos),
                             ast.FunctionName("foo")(pos),
                             distinct = false,
                             Vector.empty))
  }

  test("my.namespace.foo('test', 1 + 2)") {
    yields(
      ast.FunctionInvocation(
        ast.Namespace(List("my", "namespace"))(pos),
        ast.FunctionName("foo")(pos),
        distinct = false,
        Vector(
          ast.StringLiteral("test")(pos),
          ast.Add(ast.SignedDecimalIntegerLiteral("1")(pos), ast.SignedDecimalIntegerLiteral("2")(pos))(pos)
        )
      ))
  }

  private val pos = DummyPosition(-1)

  implicit class StringToVariable(string: String) {
    def asVar = id(string)(pos)
  }
}
