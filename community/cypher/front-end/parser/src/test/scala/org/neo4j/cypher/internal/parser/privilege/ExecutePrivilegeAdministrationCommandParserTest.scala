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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class ExecutePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grantExecutePrivilege: executePrivilegeFunc),
    ("DENY", "TO", denyExecutePrivilege: executePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecutePrivilege: executePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecutePrivilege: executePrivilegeFunc),
    ("REVOKE", "FROM", revokeExecutePrivilege: executePrivilegeFunc)
  ).foreach{
    case (verb: String, preposition: String, func: executePrivilegeFunc) =>

      Seq(
        ("EXECUTE PROCEDURE", ExecuteProcedureAction)
      ).foreach {
        case (execute, action) =>
          test(s"$verb $execute * ON DBMS $preposition role") {
            yields(func(action, List(ast.ProcedureAllQualifier()(_)), Seq(literal("role"))))
          }

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            yields(func(action, List(ast.ProcedureAllQualifier()(_)), Seq(literal("role"))))
          }

          test(s"$verb $execute apoc.procedure ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("apoc"))(_), expressions.ProcedureName("procedure")(_))(_)),
              Seq(literal("role"))))
          }

          test(s"$verb ${execute}S apoc.procedure ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("apoc"))(_), expressions.ProcedureName("procedure")(_))(_)),
              Seq(literal("role"))))
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("apoc", "math"))(_), expressions.ProcedureName("sin")(_))(_)),
              Seq(literal("role"))))
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            failsToParse
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            failsToParse
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            failsToParse
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            failsToParse
          }

          test(s"$verb $execute apoc.math.* ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("apoc", "math"))(_), expressions.ProcedureName("*")(_))(_)),
              Seq(literal("role"))))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("math"))(_), expressions.ProcedureName("sin")(_))(_),
                   ast.ProcedureQualifier(expressions.Namespace(List("math"))(_), expressions.ProcedureName("cos")(_))(_)),
              Seq(literal("role"))))
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            yields(func(
              action,
              List(ast.ProcedureQualifier(expressions.Namespace(List("apoc", "math"))(_), expressions.ProcedureName("sin")(_))(_),
                ast.ProcedureQualifier(expressions.Namespace(List("math"))(_), expressions.ProcedureName("*")(_))(_)),
              Seq(literal("role"))))
          }
      }
  }
}
