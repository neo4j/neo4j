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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.ReadSecretsAction
import org.neo4j.cypher.internal.ast.SecretAllQualifier
import org.neo4j.cypher.internal.ast.SecretQualifier
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

class ReadSecretsAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantReadSecretsPrivilege: secretsPrivilegeFunc),
    ("DENY", "TO", denyReadSecretsPrivilege: secretsPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantReadSecretsPrivilege: secretsPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyReadSecretsPrivilege: secretsPrivilegeFunc),
    ("REVOKE", "FROM", revokeReadSecretsPrivilege: secretsPrivilegeFunc)
  ).foreach {
    case (command: String, preposition: String, func: secretsPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          Seq(
            "READ SECRET",
            "READ SECRETS"
          ).foreach {
            case execute =>
              test(s"$command$immutableString $execute * ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ =>
                    _.toAst(Statements(Seq(func(
                      ReadSecretsAction,
                      List(secretAllQualifier),
                      Seq(literalRole),
                      immutable
                    )(pos))))
                }
              }

              test(s"$command$immutableString $execute 'sec1' ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.toAst(Statements(Seq(func(
                      ReadSecretsAction,
                      List(readSecretQualifier("sec1")),
                      Seq(literalRole),
                      immutable
                    )(pos))))
                }
              }

              test(s"""$command$immutableString $execute '*' ON DBMS $preposition role""") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ =>
                    _.toAst(Statements(Seq(func(
                      ReadSecretsAction,
                      List(readSecretQualifier("*")),
                      Seq(literalRole),
                      immutable
                    )(pos))))
                }
              }

              test(s"""$command$immutableString $execute '' ON DBMS $preposition role""") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ =>
                    _.toAst(Statements(Seq(func(
                      ReadSecretsAction,
                      List(readSecretQualifier("")),
                      Seq(literalRole),
                      immutable
                    )(pos))))
                }
              }

              test(s"""$command$immutableString $execute $$foo ON DBMS $preposition role""") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ =>
                    _.toAst(Statements(Seq(func(
                      ReadSecretsAction,
                      List(readSecretQualifier(Right(paramFoo))),
                      Seq(literalRole),
                      immutable
                    )(pos))))
                }
              }

              test(s"$command$immutableString $execute ´sec1´ ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                }
              }

              test(s"$command$immutableString $execute 'sec1', 'sec2' ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                }
              }

              test(s"$command$immutableString $execute 'sec1', $$foo ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                }
              }

              test(s"$command$immutableString $execute ['sec1', 'sec2'] ON DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                }
              }

              test(s"$command$immutableString $execute * DBMS $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input 'DBMS': expected 'ON DBMS'")
                }
              }

              test(s"$command$immutableString $execute * ON DATABASE $preposition role") {
                parsesIn[Statements] {
                  case Cypher5 =>
                    _.withSyntaxErrorContaining(command)
                  case _ => _.throws[SyntaxException].withMessageContaining("Invalid input 'DATABASE': expected 'DBMS'")
                }
              }
          }
      }

  }

  private def readSecretQualifier(secret: Either[String, Parameter]): InputPosition => SecretQualifier =
    SecretQualifier(secret)(_)
  private def secretAllQualifier: InputPosition => SecretAllQualifier = SecretAllQualifier()(_)
}
