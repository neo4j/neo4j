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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.DummyPosition

class FunctionInvocationParserTest
  extends ParserAstTest[expressions.FunctionInvocation]
    with Expressions
    with Literals
    with Base
    with ProcedureCalls  {

  implicit val parser = FunctionInvocation

  test("foo()") {
    yields(expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("foo")(pos), distinct = false, Vector.empty))
  }

  test("foo('test', 1 + 2)") {
    yields(expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("foo")(pos), distinct = false, Vector(
      expressions.StringLiteral("test")(pos),
      expressions.Add(
        expressions.SignedDecimalIntegerLiteral("1")(pos),
        expressions.SignedDecimalIntegerLiteral("2")(pos))(pos)
    )))
  }
  test("my.namespace.foo()") {
    yields(expressions.FunctionInvocation(expressions.Namespace(List("my", "namespace"))(pos), expressions.FunctionName("foo")(pos), distinct = false, Vector.empty))
  }

  test("my.namespace.foo('test', 1 + 2)") {
    yields(expressions.FunctionInvocation(expressions.Namespace(List("my", "namespace"))(pos), expressions.FunctionName("foo")(pos), distinct = false, Vector(
      expressions.StringLiteral("test")(pos),
      expressions.Add(
        expressions.SignedDecimalIntegerLiteral("1")(pos),
        expressions.SignedDecimalIntegerLiteral("2")(pos))(pos)
    )))
  }


  private val pos = DummyPosition(-1)

  implicit class StringToVariable(string: String) {
    def asVar = id(string)(pos)
  }
}
