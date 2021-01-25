/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class ExecuteFunctionPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  private val apocString = "apoc"
  private val mathString = "math"

  Seq(
    ("GRANT", "TO", grantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("DENY", "TO", denyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE", "FROM", revokeExecuteFunctionPrivilege: executeFunctionPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: executeFunctionPrivilegeFunc) =>

      Seq(
        ("EXECUTE FUNCTION", ExecuteFunctionAction),
        ("EXECUTE USER FUNCTION", ExecuteFunctionAction),
        ("EXECUTE USER DEFINED FUNCTION", ExecuteFunctionAction),
        ("EXECUTE BOOSTED FUNCTION", ExecuteBoostedFunctionAction),
        ("EXECUTE BOOSTED USER FUNCTION", ExecuteBoostedFunctionAction),
        ("EXECUTE BOOSTED USER DEFINED FUNCTION", ExecuteBoostedFunctionAction)
      ).foreach {
        case (execute, action) =>

          test(s"$verb $execute * ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.function ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(apocString), "function")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S apoc.function ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(apocString), "function")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(apocString, mathString), "sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*apoc")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List("*"), "sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(apocString, "*", mathString), "*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(mathString), "*n")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List("mat?", "a.\n"), "*n")), Seq(literalRole)))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(mathString), "sin"), functionQualifier(List(mathString), "cos")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier(List(apocString, mathString), "sin"), functionQualifier(List(mathString), "*")), Seq(literalRole)))
          }

          test(s"$verb $execute * $preposition role") {
            failsToParse
          }

          test(s"$verb $execute * ON DATABASE * $preposition role") {
            failsToParse
          }
      }
  }

  private def functionQualifier(funcName: String): InputPosition => FunctionQualifier = functionQualifier(List.empty, funcName)

  private def functionQualifier(nameSpace: List[String], funcName: String): InputPosition => FunctionQualifier =
    FunctionQualifier(expressions.Namespace(nameSpace)(_), expressions.FunctionName(funcName)(_))(_)
}
