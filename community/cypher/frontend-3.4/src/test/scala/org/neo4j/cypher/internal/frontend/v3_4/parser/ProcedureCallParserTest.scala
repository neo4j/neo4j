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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.aux.v3_4.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.v3_4.{expressions => exp}

class ProcedureCallParserTest
  extends ParserAstTest[ast.UnresolvedCall]
    with Expressions
    with Literals
    with Base
    with ProcedureCalls {

  implicit val parser = Call

  test("CALL foo") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos)))
  }

  test("CALL foo()") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos), Some(Seq.empty)))
  }

  test("CALL foo('Test', 1+2)") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos),
                              Some(Vector(
        exp.StringLiteral("Test")(pos),
        exp.Add(
          exp.SignedDecimalIntegerLiteral("1")(pos),
          exp.SignedDecimalIntegerLiteral("2")(pos))(pos)
      )))
    )
  }

  test("CALL foo.bar.baz('Test', 1+2)") {
    yields(ast.UnresolvedCall(exp.Namespace(List("foo", "bar"))(pos), exp.ProcedureName("baz")(pos),
                              Some(Vector(
        exp.StringLiteral("Test")(pos),
        exp.Add(
          exp.SignedDecimalIntegerLiteral("1")(pos),
          exp.SignedDecimalIntegerLiteral("2")(pos))(pos)
      )))
    )
  }

  test("CALL foo YIELD bar") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos), None, Some(ast.ProcedureResult.from
    (result("bar"))(pos))))
  }

  test("CALL foo YIELD bar, baz") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos), None, Some(ast.ProcedureResult.from
    (result("bar"), result("baz"))(pos))))
  }

  test("CALL foo() YIELD bar") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos), Some(Seq.empty), Some
    (ast.ProcedureResult.from(result("bar"))(pos))))
  }

  test("CALL foo() YIELD bar, baz") {
    yields(ast.UnresolvedCall(exp.Namespace()(pos), exp.ProcedureName("foo")(pos), Some(Seq.empty), Some
    (ast.ProcedureResult.from(result("bar"), result("baz"))(pos))))
  }

  private def result(name: String): ast.ProcedureResultItem =
    ast.ProcedureResultItem(exp.Variable(name)(pos))(pos)

  private def result(output: String, name: String): ast.ProcedureResultItem =
    ast.ProcedureResultItem(exp.ProcedureOutput(output)(pos), exp.Variable(name)(pos))(pos)

  private implicit val pos = DummyPosition(-1)

  implicit class StringToVariable(string: String) {
    def asVar = id(string)(pos)
  }
}
