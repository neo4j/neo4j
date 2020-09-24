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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class ExecuteProcedurePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  private val apocString = "apoc"
  private val mathString = "math"

  Seq(
    ("GRANT", "TO", grantExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("DENY", "TO", denyExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE", "FROM", revokeExecuteProcedurePrivilege: executeProcedurePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: executeProcedurePrivilegeFunc) =>

      Seq(
        ("EXECUTE PROCEDURE", ExecuteProcedureAction),
        ("EXECUTE BOOSTED PROCEDURE", ExecuteBoostedProcedureAction)
      ).foreach {
        case (execute, action) =>

          test(s"$verb $execute * ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.procedure ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString), "procedure")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S apoc.procedure ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString), "procedure")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString, mathString), "sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*apoc")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString, "*", mathString), "*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(mathString), "*n")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List("mat?", "a.\n"), "*n")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List("*"), "sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString, mathString), "*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(mathString), "sin"), procedureQualifier(List(mathString), "cos")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier(List(apocString, mathString), "sin"), procedureQualifier(List(mathString), "*")), Seq(literalRole)))
          }

          test(s"$verb $execute * $preposition role") {
            failsToParse
          }

          test(s"$verb $execute * ON DATABASE * $preposition role") {
            failsToParse
          }
      }
  }

  Seq(
    ("GRANT", "TO", grantDbmsPrivilege: dbmsPrivilegeFunc),
    ("DENY", "TO", denyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE", "FROM", revokeDbmsPrivilege: dbmsPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: dbmsPrivilegeFunc) =>
      Seq(
        "EXECUTE ADMIN PROCEDURES",
        "EXECUTE ADMINISTRATOR PROCEDURES"
      ).foreach {
        command =>

          test(s"$verb $command ON DBMS $preposition role") {
            yields(func(ExecuteAdminProcedureAction, Seq(literalRole)))
          }

          test(s"$verb $command * ON DBMS $preposition role") {
            failsToParse
          }

          test(s"$verb $command ON DATABASE * $preposition role") {
            failsToParse
          }
          
      }

      test(s"$verb EXECUTE ADMIN PROCEDURE ON DBMS $preposition role") {
        failsToParse
      }
  }

  private def procedureQualifier(procName: String): InputPosition => ProcedureQualifier = procedureQualifier(List.empty, procName)

  private def procedureQualifier(nameSpace: List[String], procName: String): InputPosition => ProcedureQualifier =
    ProcedureQualifier(expressions.Namespace(nameSpace)(_), expressions.ProcedureName(procName)(_))(_)
}
