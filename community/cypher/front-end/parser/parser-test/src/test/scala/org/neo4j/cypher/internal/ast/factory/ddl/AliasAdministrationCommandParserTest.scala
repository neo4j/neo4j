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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class AliasAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  override protected def ignorePrettifier: Boolean = true

  // CREATE ALIAS

  test("CREATE ALIAS alias FOR DATABASE target") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("alias"),
      namespacedName("target"),
      IfExistsThrowError
    )(defaultPos))
  }

  test("CREATE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("alias"),
      namespacedName("target"),
      IfExistsDoNothing
    )(defaultPos))
  }

  test("CREATE OR REPLACE ALIAS alias FOR DATABASE target") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("alias"),
      namespacedName("target"),
      IfExistsReplace
    )(defaultPos))
  }

  test("CREATE OR REPLACE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias"),
        namespacedName(fromCypher5, "target"),
        IfExistsInvalidSyntax
      )(defaultPos)
    )
  }

  test("CREATE ALIAS alias.name FOR DATABASE db.name") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias", "name"),
        namespacedName(fromCypher5, "db", "name"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS alias . name FOR DATABASE db.name") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias", "name"),
        namespacedName(fromCypher5, "db", "name"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS IF FOR DATABASE db.name") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "IF"),
        namespacedName(fromCypher5, "db", "name"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS composite.alias FOR DATABASE db") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "composite", "alias"),
        namespacedName(fromCypher5, "db"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS alias.alias FOR DATABASE db") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias", "alias"),
        namespacedName(fromCypher5, "db"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS alias.if IF NOT EXISTS FOR DATABASE db") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias", "if"),
        namespacedName(fromCypher5, "db"),
        IfExistsDoNothing
      )(defaultPos)
    )
  }

  test("CREATE ALIAS very.long.alias IF NOT EXISTS FOR DATABASE db") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "very", "long", "alias"),
        namespacedName(fromCypher5, "db"),
        IfExistsDoNothing
      )(defaultPos)
    )
  }

  test("CREATE ALIAS `a`.b.c.d IF NOT EXISTS FOR DATABASE db") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateLocalDatabaseAlias(
            NamespacedName(List("b", "c", "d"), Some("a"))(_),
            NamespacedName(List("db"), None)(_),
            IfExistsDoNothing
          )(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`a`.b.c.d'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))
            |"CREATE ALIAS `a`.b.c.d IF NOT EXISTS FOR DATABASE db"
            |              ^""".stripMargin
        )
    }
  }

  test("CREATE ALIAS a.b.c.`d` IF NOT EXISTS FOR DATABASE db") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateLocalDatabaseAlias(
            namespacedName("a", "b", "c", "d"),
            namespacedName("db"),
            IfExistsDoNothing
          )(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference 'a.b.c.`d`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))
            |"CREATE ALIAS a.b.c.`d` IF NOT EXISTS FOR DATABASE db"
            |              ^""".stripMargin
        )
    }
  }

  test("CREATE ALIAS alias.for FOR DATABASE db") {
    assertAstVersionBased(fromCypher5 =>
      CreateLocalDatabaseAlias(
        namespacedName(fromCypher5, "alias", "for"),
        namespacedName(fromCypher5, "db"),
        IfExistsThrowError
      )(defaultPos)
    )
  }

  test("CREATE ALIAS $alias FOR DATABASE $target") {
    assertAst(CreateLocalDatabaseAlias(
      stringParamName("alias"),
      stringParamName("target"),
      IfExistsThrowError
    )(defaultPos))
  }

  test("CREATE ALIAS alias FOR DATABASE target PROPERTIES { key:'value', anotherkey:'anotherValue' }") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("alias"),
      namespacedName("target"),
      IfExistsThrowError,
      Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anotherValue"))))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target PROPERTIES { key:12.5, anotherkey: { innerKey: 17 }, another: [1,2,'hi'] }"""
  ) {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        properties =
          Some(Left(Map(
            "key" -> literalFloat(12.5),
            "anotherkey" -> mapOf("innerKey" -> literalInt(17)),
            "another" -> listOf(literalInt(1), literalInt(2), literalString("hi"))
          )))
      )(defaultPos)
    )
  }

  test("""CREATE ALIAS alias FOR DATABASE target PROPERTIES { }""") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        properties =
          Some(Left(Map()))
      )(defaultPos)
    )
  }

  test("""CREATE ALIAS alias FOR DATABASE target PROPERTIES $props""") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  test("CREATE ALIAS `Mal#mö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("Mal#mö"),
      namespacedName("db1"),
      IfExistsThrowError
    )(defaultPos))
  }

  test("CREATE ALIAS `#Malmö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("#Malmö"),
      namespacedName("db1"),
      IfExistsThrowError
    )(defaultPos))
  }

  test("CREATE ALIAS IF") {
    failsParsing[Statements].withMessage(
      """Invalid input '': expected a database name, 'FOR DATABASE' or 'IF NOT EXISTS' (line 1, column 16 (offset: 15))
        |"CREATE ALIAS IF"
        |                ^""".stripMargin
    )
  }

  test("CREATE ALIAS") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name, a graph pattern or a parameter (line 1, column 13 (offset: 12))
        |"CREATE ALIAS"
        |             ^""".stripMargin
    )
  }

  test("CREATE ALIAS #Malmö FOR DATABASE db1") {
    failsParsing[Statements].withMessage(
      """Invalid input '#': expected a database name, a graph pattern or a parameter (line 1, column 14 (offset: 13))
        |"CREATE ALIAS #Malmö FOR DATABASE db1"
        |              ^""".stripMargin
    )
  }

  test("CREATE ALIAS Mal#mö FOR DATABASE db1") {
    failsParsing[Statements].withMessage(
      """Invalid input '#': expected a database name, 'FOR DATABASE' or 'IF NOT EXISTS' (line 1, column 17 (offset: 16))
        |"CREATE ALIAS Mal#mö FOR DATABASE db1"
        |                 ^""".stripMargin
    )
  }

  test("CREATE ALIAS name FOR DATABASE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name or a parameter (line 1, column 31 (offset: 30))
        |"CREATE ALIAS name FOR DATABASE"
        |                               ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'PROPERTY': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
        |"CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }"
        |                                       ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTIES""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or '{' (line 1, column 49 (offset: 48))
        |"CREATE ALIAS name FOR DATABASE target PROPERTIES"
        |                                                 ^""".stripMargin
    )
  }

  test("CREATE ALIAS `a`.`b`.`c` FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS `a`.b.`c` FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.b.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.b.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS `a`.b.c.`d` FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.b.c.`d`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.b.c.`d`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.b.c.`d`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.b.c.`d`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS a.`b`.`c` FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS `a`.`b`.c FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS a.`b`.c FOR DATABASE db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS `a`.`b` FOR DATABASE `db.cd`.`ef.gh`.d") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``db.cd`.`ef.gh`.d` for name. Expected name to contain at most two components separated by `.`. (line 1, column 35 (offset: 34))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`db.cd`.`ef.gh`.d' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          """Incorrectly formatted graph reference '`a`.`b`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))
            |"CREATE ALIAS `a`.`b` FOR DATABASE `db.cd`.`ef.gh`.d"
            |              ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("CREATE ALIAS name FOR DATABASE target DEFAULT LANGUAGE CYPHER 5") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DEFAULT': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
        |"CREATE ALIAS name FOR DATABASE target DEFAULT LANGUAGE CYPHER 5"
        |                                       ^""".stripMargin
    )
  }

  test("CREATE ALIAS name FOR DATABASE target PROPERTIES {} DEFAULT LANGUAGE CYPHER 25") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DEFAULT': expected <EOF> (line 1, column 53 (offset: 52))
        |"CREATE ALIAS name FOR DATABASE target PROPERTIES {} DEFAULT LANGUAGE CYPHER 25"
        |                                                     ^""".stripMargin
    )
  }

  test("CREATE ALIAS name FOR DATABASE target SET DEFAULT LANGUAGE CYPHER 25") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
        |"CREATE ALIAS name FOR DATABASE target SET DEFAULT LANGUAGE CYPHER 25"
        |                                       ^""".stripMargin
    )
  }

  test("CREATE ALIAS alias FOR DATABASE target USER user PASSWORD 'password'") {
    failsParsing[Statements]
      .withSyntaxError(
        """Invalid input 'USER': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 40 (offset: 39))
          |"CREATE ALIAS alias FOR DATABASE target USER user PASSWORD 'password'"
          |                                        ^""".stripMargin
      ).withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_42I06,
        "error: syntax error or access rule violation - invalid input. Invalid input 'USER', expected: a database name, 'AT', 'PROPERTIES' or <EOF>."
      ))
  }

  test("CREATE ALIAS alias FOR DATABASE target OIDC CREDENTIAL FORWARDING") {
    failsParsing[Statements]
      .withSyntaxError(
        """Invalid input 'OIDC': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 40 (offset: 39))
          |"CREATE ALIAS alias FOR DATABASE target OIDC CREDENTIAL FORWARDING"
          |                                        ^""".stripMargin
      ).withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_42I06,
        "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: a database name, 'AT', 'PROPERTIES' or <EOF>."
      ))
  }

  // CREATE REMOTE ALIAS

  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING""") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OIDC': expected 'USER' (line 1, column 65 (offset: 64))
            |"CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING"
            |                                                                 ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'USER'."
            )
          )
      case _ => _.toAstPositioned(
          CreateRemoteDatabaseAlias(
            namespacedName("name"),
            namespacedName("target"),
            IfExistsThrowError,
            Left("neo4j://serverA:7687"),
            OidcCredentialForwarding()(pos)
          )(defaultPos)
        )
    }
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687"""") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '': expected 'USER' (line 1, column 64 (offset: 63))
            |"CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687""
            |                                                                ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input '', expected: 'USER'."
            )
          )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'OIDC CREDENTIAL FORWARDING' or 'USER' (line 1, column 64 (offset: 63))
            |"CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687""
            |                                                                ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input '', expected: 'OIDC CREDENTIAL FORWARDING' or 'USER'."
            )
          )
    }
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |OIDC CREDENTIAL FORWARDING""".stripMargin
  ) {
    val offset = if (testName.contains("\r")) 95 else 94
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          s"""Invalid input 'OIDC': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF> (line 2, column 1 (offset: $offset))
             |"OIDC CREDENTIAL FORWARDING"
             | ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF>."
            )
          )
      case _ => _.withSyntaxError(
          s"""Invalid input 'OIDC': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF> (line 2, column 1 (offset: $offset))
             |"OIDC CREDENTIAL FORWARDING"
             | ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF>."
            )
          )
    }
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING
      |USER user PASSWORD 'password'""".stripMargin
  ) {
    val offsetForLine2 = if (testName.contains("\r")) 92 else 91
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OIDC': expected 'USER' (line 1, column 65 (offset: 64))
            |"CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING"
            |                                                                 ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'USER'."
            )
          )
      case _ => _.withSyntaxError(
          s"""Invalid input 'USER': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF> (line 2, column 1 (offset: $offsetForLine2))
             |"USER user PASSWORD 'password'"
             | ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'USER', expected: 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF>."
            )
          )
    }
  }

  test(
    """CREATE ALIAS namespace.`name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateRemoteDatabaseAlias(
            namespacedName("namespace", "name.illegal"),
            namespacedName("target"),
            IfExistsThrowError,
            Left("neo4j://serverA:7687"),
            RemoteAliasStoredCredentials(
              literalString("user"),
              sensitiveLiteral("password")
            )(pos)
          )(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference 'namespace.`name.illegal`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 14 (offset: 13))
            |"CREATE ALIAS namespace.`name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"
            |              ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'namespace.`name.illegal`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test(
    """CREATE ALIAS `name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""".stripMargin
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name.illegal"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test("CREATE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT '' USER `` PASSWORD ''""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left(""),
      RemoteAliasStoredCredentials(
        literalString(""),
        sensitiveLiteral("")
      )(pos)
    )(defaultPos))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      Right(stringParam("url")),
      RemoteAliasStoredCredentials(
        stringParam("user"),
        pwParam("password")
      )(pos)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsDoNothing,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS composite.name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |PROPERTIES { key:'value', anotherkey:'anotherValue' }""".stripMargin
  ) {
    assertAstVersionBased(fromCypher5 =>
      CreateRemoteDatabaseAlias(
        namespacedName(fromCypher5, "composite", "name"),
        namespacedName(fromCypher5, "target"),
        IfExistsDoNothing,
        Left("neo4j://serverA:7687"),
        RemoteAliasStoredCredentials(
          literalString("user"),
          sensitiveLiteral("password")
        )(pos),
        None,
        Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anotherValue"))))
      )(defaultPos)
    )
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |PROPERTIES { key:12.5, anotherkey: { innerKey: 17 }, another: [1,2,'hi'] }""".stripMargin
  ) {
    assertAst(
      CreateRemoteDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        Left("neo4j://serverA:7687"),
        RemoteAliasStoredCredentials(
          literalString("user"),
          sensitiveLiteral("password")
        )(pos),
        None,
        properties =
          Some(Left(Map(
            "key" -> literalFloat(12.5),
            "anotherkey" -> mapOf("innerKey" -> literalInt(17)),
            "another" -> listOf(literalInt(1), literalInt(2), literalString("hi"))
          )))
      )(defaultPos)
    )
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password' PROPERTIES { }"""
  ) {
    assertAst(
      CreateRemoteDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        Left("neo4j://serverA:7687"),
        RemoteAliasStoredCredentials(
          literalString("user"),
          sensitiveLiteral("password")
        )(pos),
        None,
        properties =
          Some(Left(Map()))
      )(defaultPos)
    )
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password' PROPERTIES $props"""
  ) {
    assertAst(
      CreateRemoteDatabaseAlias(
        namespacedName("alias"),
        namespacedName("target"),
        IfExistsThrowError,
        Left("neo4j://serverA:7687"),
        RemoteAliasStoredCredentials(
          literalString("user"),
          sensitiveLiteral("password")
        )(pos),
        None,
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING PROPERTIES $props"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OIDC': expected 'USER' (line 1, column 66 (offset: 65))
            |"CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING PROPERTIES $props"
            |                                                                  ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'USER'."
            )
          )
      case _ => _.toAstPositioned(CreateRemoteDatabaseAlias(
          namespacedName("alias"),
          namespacedName("target"),
          IfExistsThrowError,
          Left("neo4j://serverA:7687"),
          OidcCredentialForwarding()(pos),
          None,
          properties =
            Some(Right(parameter("props", CTMap)))
        )(defaultPos))
    }
  }

  test("CREATE OR REPLACE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsReplace,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test(
    "CREATE OR REPLACE ALIAS name IF NOT EXISTS FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'"
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsInvalidSyntax,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING DRIVER { ssl_enforced: true }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OIDC': expected 'USER' (line 1, column 65 (offset: 64))
            |"CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" OIDC CREDENTIAL FORWARDING DRIVER { ssl_enforced: true }"
            |                                                                 ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42I06,
              "error: syntax error or access rule violation - invalid input. Invalid input 'OIDC', expected: 'USER'."
            )
          )
      case _ => _.toAstPositioned(CreateRemoteDatabaseAlias(
          namespacedName("name"),
          namespacedName("target"),
          IfExistsThrowError,
          Left("neo4j://serverA:7687"),
          OidcCredentialForwarding()(pos),
          Some(Left(Map(
            "ssl_enforced" -> trueLiteral
          )))
        )(defaultPos))
    }
  }

  test(
    """CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password' DRIVER { ssl_enforced: true }"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsDoNothing,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  private val CreateRemoteDatabaseAliasWithDriverSettings =
    """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |DRIVER
      |{
      |    ssl_enforced: true,
      |    connection_timeout: duration('PT1S'),
      |    connection_max_lifetime: duration('PT1S'),
      |    connection_pool_acquisition_timeout: duration('PT1S'),
      |    connection_pool_idle_test: duration('PT1S'),
      |    connection_pool_max_size: 1000,
      |	   logging_level: "DEBUG"
      |}
      |""".stripMargin

  test(CreateRemoteDatabaseAliasWithDriverSettings) {
    val durationExpression = function("duration", literalString("PT1S"))

    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
        "connection_timeout" -> durationExpression,
        "connection_max_lifetime" -> durationExpression,
        "connection_pool_acquisition_timeout" -> durationExpression,
        "connection_pool_idle_test" -> durationExpression,
        "connection_pool_max_size" -> literalInt(1000),
        "logging_level" -> literalString("DEBUG")
      )))
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER { foo: 1.0 }""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("bar"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map(
        "foo" -> literalFloat(1.0)
      )))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER { foo: 1.0 } PROPERTIES { bar: true }"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("bar"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map(
        "foo" -> literalFloat(1.0)
      ))),
      Some(Left(Map("bar" -> trueLiteral)))
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("bar"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password DRIVER $driver""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      Right(stringParam("url")),
      RemoteAliasStoredCredentials(
        stringParam("user"),
        pwParam("password")
      )(pos),
      Some(Right(parameter("driver", CTMap)))
    )(defaultPos))
  }

  test("""CREATE ALIAS driver FOR DATABASE at AT "driver" USER driver PASSWORD "driver" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("driver"),
      namespacedName("at"),
      IfExistsThrowError,
      Left("driver"),
      RemoteAliasStoredCredentials(
        literalString("driver"),
        sensitiveLiteral("driver")
      )(pos),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 5""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("url"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      defaultLanguage = Some(CypherVersion.Cypher5)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} DEFAULT LANGUAGE CYPHER 25"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("url"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map.empty)),
      defaultLanguage = Some(CypherVersion.Cypher25)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 25 PROPERTIES {}"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("url"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      properties = Some(Left(Map.empty)),
      defaultLanguage = Some(CypherVersion.Cypher25)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} DEFAULT LANGUAGE CYPHER 5 PROPERTIES {}"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("url"),
      RemoteAliasStoredCredentials(
        literalString("user"),
        sensitiveLiteral("password")
      )(pos),
      Some(Left(Map.empty)),
      Some(Left(Map.empty)),
      defaultLanguage = Some(CypherVersion.Cypher5)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS namespace.name.illegal FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withMessageStart(
          "'.' is not a valid character in the remote alias name 'namespace.name.illegal'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`. (line 1, column 14 (offset: 13))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'namespace.name.illegal' for remote alias name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N82,
                "error: data exception - input contains invalid characters. Input 'namespace.name.illegal' contains invalid characters for remote alias name. Special characters may require that the input is quoted using backticks."
              )
          )
      case _ => _.toAstPositioned(
          CreateRemoteDatabaseAlias(
            NamespacedName(List("namespace.name.illegal"), None)(_),
            NamespacedName(List("target"), None)(_),
            ifExistsDo = IfExistsThrowError,
            url = Left("neo4j://serverA:7687"),
            RemoteAliasStoredCredentials(
              literalString("user"),
              sensitiveLiteral("password")
            )(pos)
          )(defaultPos)
        )
    }

  }

  test("""CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'neo4j': expected a parameter or a string (line 1, column 42 (offset: 41))
        |"CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'"
        |                                          ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS composite.name FOR DATABASE target AT "neo4j://serverA:7687"
      |PROPERTIES { key:'value', anotherkey:'anotherValue' }
      |USER user PASSWORD 'password'""".stripMargin
  ) {
    val offset = if (testName.contains("\r\n")) "75" else "74" // Windows line endings changes the offset...
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          s"""Invalid input 'PROPERTIES': expected 'USER' (line 2, column 1 (offset: $offset))
             |"PROPERTIES { key:'value', anotherkey:'anotherValue' }"
             | ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          s"""Invalid input 'PROPERTIES': expected 'OIDC CREDENTIAL FORWARDING' or 'USER' (line 2, column 1 (offset: $offset))
             |"PROPERTIES { key:'value', anotherkey:'anotherValue' }"
             | ^""".stripMargin
        )
    }
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" PROPERTIES { bar: true } DRIVER { foo: 1.0 }"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DRIVER': expected <EOF> (line 1, column 103 (offset: 102))
        |"CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" PROPERTIES { bar: true } DRIVER { foo: 1.0 }"
        |                                                                                                       ^""".stripMargin
    )
  }

  test("Should fail to parse CREATE ALIAS with driver settings but no remote url") {
    "CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }" should notParse[Statements].withSyntaxError(
      """Invalid input 'DRIVER': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
        |"CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }"
        |                                       ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }""") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'USER' (line 1, column 48 (offset: 47))
            |"CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }"
            |                                                ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'OIDC CREDENTIAL FORWARDING' or 'USER' (line 1, column 48 (offset: 47))
            |"CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }"
            |                                                ^""".stripMargin
        )
    }
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or '{' (line 1, column 84 (offset: 83))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER"
        |                                                                                    ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'PROPERTY': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF> (line 1, column 78 (offset: 77))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }"
        |                                                                              ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or '{' (line 1, column 88 (offset: 87))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES"
        |                                                                                        ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" DEFAULT LANGUAGE CYPHER 5 USER user PASSWORD 'password' DRIVER {} PROPERTIES {}"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'DEFAULT': expected 'USER' (line 1, column 48 (offset: 47))
            |"CREATE ALIAS name FOR DATABASE target AT "url" DEFAULT LANGUAGE CYPHER 5 USER user PASSWORD 'password' DRIVER {} PROPERTIES {}"
            |                                                ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'DEFAULT': expected 'OIDC CREDENTIAL FORWARDING' or 'USER' (line 1, column 48 (offset: 47))
            |"CREATE ALIAS name FOR DATABASE target AT "url" DEFAULT LANGUAGE CYPHER 5 USER user PASSWORD 'password' DRIVER {} PROPERTIES {}"
            |                                                ^""".stripMargin
        )
    }
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user DEFAULT LANGUAGE CYPHER 5 PASSWORD 'password' DRIVER {} PROPERTIES {}"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DEFAULT': expected 'PASSWORD' (line 1, column 58 (offset: 57))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user DEFAULT LANGUAGE CYPHER 5 PASSWORD 'password' DRIVER {} PROPERTIES {}"
        |                                                          ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 5 DRIVER {} PROPERTIES {}"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DRIVER': expected 'PROPERTIES' or <EOF> (line 1, column 104 (offset: 103))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 5 DRIVER {} PROPERTIES {}"
        |                                                                                                        ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} PROPERTIES {} DEFAULT LANGUAGE CYPHER 5"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DEFAULT': expected <EOF> (line 1, column 102 (offset: 101))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} PROPERTIES {} DEFAULT LANGUAGE CYPHER 5"
        |                                                                                                      ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' SET DEFAULT LANGUAGE CYPHER 5"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PROPERTIES' or <EOF> (line 1, column 78 (offset: 77))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' SET DEFAULT LANGUAGE CYPHER 5"
        |                                                                              ^""".stripMargin
    )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} SET DEFAULT LANGUAGE CYPHER 5"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected 'DEFAULT LANGUAGE CYPHER', 'PROPERTIES' or <EOF> (line 1, column 88 (offset: 87))
        |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DRIVER {} SET DEFAULT LANGUAGE CYPHER 5"
        |                                                                                        ^""".stripMargin
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 42""") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '42' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '42'. Valid Cypher versions are: 5, 25 (line 1, column 102 (offset: 101))
          |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 42"
          |                                                                                                      ^""".stripMargin
      )
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 42*10"""
  ) {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '42' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '42'. Valid Cypher versions are: 5, 25 (line 1, column 102 (offset: 101))
          |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD 'password' DEFAULT LANGUAGE CYPHER 42*10"
          |                                                                                                      ^""".stripMargin
      )
  }

  // DROP ALIAS

  test("DROP ALIAS name FOR DATABASE") {
    assertAst(DropDatabaseAlias(namespacedName("name"), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS $name FOR DATABASE") {
    assertAst(DropDatabaseAlias(stringParamName("name"), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS name IF EXISTS FOR DATABASE") {
    assertAst(DropDatabaseAlias(namespacedName("name"), ifExists = true)(defaultPos))
  }

  test("DROP ALIAS wait FOR DATABASE") {
    assertAst(DropDatabaseAlias(namespacedName("wait"), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS nowait FOR DATABASE") {
    assertAst(DropDatabaseAlias(namespacedName("nowait"), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS composite.name FOR DATABASE") {
    assertAstVersionBased(fromCypher5 =>
      DropDatabaseAlias(namespacedName(fromCypher5, "composite", "name"), ifExists = false)(defaultPos)
    )
  }

  test("DROP ALIAS composite.`dotted.name` FOR DATABASE") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          DropDatabaseAlias(namespacedName("composite", "dotted.name"), ifExists = false)(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference 'composite.`dotted.name`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"DROP ALIAS composite.`dotted.name` FOR DATABASE"
            |            ^""".stripMargin
        )
    }
  }

  test("DROP ALIAS `dotted.composite`.name FOR DATABASE") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          DropDatabaseAlias(namespacedName("dotted.composite", "name"), ifExists = false)(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`dotted.composite`.name'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"DROP ALIAS `dotted.composite`.name FOR DATABASE"
            |            ^""".stripMargin
        )
    }
  }

  test("DROP ALIAS name") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name, 'FOR DATABASE' or 'IF EXISTS' (line 1, column 16 (offset: 15))
        |"DROP ALIAS name"
        |                ^""".stripMargin
    )
  }

  test("DROP ALIAS name IF EXISTS") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'FOR DATABASE' (line 1, column 26 (offset: 25))
        |"DROP ALIAS name IF EXISTS"
        |                          ^""".stripMargin
    )
  }

  test("DROP ALIAS `a`.`b`.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("DROP ALIAS `a`.b.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.b.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.b.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("DROP ALIAS a.`b`.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("DROP ALIAS `a`.`b`.c FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("DROP ALIAS a.`b`.c FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  // ALTER ALIAS

  test("ALTER ALIAS name SET DATABASE TARGET db") {
    assertAst(AlterLocalDatabaseAlias(namespacedName("name"), Some(namespacedName("db")))(defaultPos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE TARGET db") {
    assertAst(
      AlterLocalDatabaseAlias(namespacedName("name"), Some(namespacedName("db")), ifExists = true)(defaultPos)
    )
  }

  test("ALTER ALIAS $name SET DATABASE TARGET $db") {
    assertAst(
      AlterLocalDatabaseAlias(stringParamName("name"), Some(stringParamName("db")))(defaultPos)
    )
  }

  test("ALTER ALIAS name.hej SET DATABASE TARGET db") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          AlterLocalDatabaseAlias(
            NamespacedName(List("hej"), Some("name"))(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          AlterLocalDatabaseAlias(
            NamespacedName(List("name.hej"), None)(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            None
          )(pos)
        )
    }
  }

  test("ALTER ALIAS name.hej.a SET DATABASE TARGET db") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          AlterLocalDatabaseAlias(
            NamespacedName(List("hej", "a"), Some("name"))(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          AlterLocalDatabaseAlias(
            NamespacedName(List("name.hej.a"), None)(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            None
          )(pos)
        )
    }
  }

  test("ALTER ALIAS $name if exists SET DATABASE TARGET $db") {
    assertAst(AlterLocalDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("db")),
      ifExists = true
    )(defaultPos))
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTIES { key:'value', anotherkey:'anothervalue' }""") {
    assertAst(
      AlterLocalDatabaseAlias(
        namespacedName("name"),
        None,
        properties =
          Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anothervalue"))))
      )(defaultPos)
    )
  }

  test("ALTER ALIAS name if exists SET db TARGET") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'db': expected 'DATABASE' (line 1, column 32 (offset: 31))
        |"ALTER ALIAS name if exists SET db TARGET"
        |                                ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET TARGET db") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'TARGET': expected 'DATABASE' (line 1, column 22 (offset: 21))
        |"ALTER ALIAS name SET TARGET db"
        |                      ^""".stripMargin
    )
  }

  test("ALTER DATABASE ALIAS name SET TARGET db") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'name': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 22 (offset: 21))
        |"ALTER DATABASE ALIAS name SET TARGET db"
        |                      ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET DATABASE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 30 (offset: 29))
        |"ALTER ALIAS name SET DATABASE"
        |                              ^""".stripMargin
    )
  }

  test("ALTER RANDOM name") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'RANDOM': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
            |"ALTER RANDOM name"
            |       ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'RANDOM': expected 'ALIAS', 'CURRENT', 'DATABASE', 'AUTH RULE', 'SERVER', 'USER' or 'USERS' (line 1, column 7 (offset: 6))
            |"ALTER RANDOM name"
            |       ^""".stripMargin
        )
    }
  }

  test("ALTER ALIAS `a`.`b`.`c` SET DATABASE TARGET db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("ALTER ALIAS `a`.b.`c` SET DATABASE TARGET db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.b.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.b.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("ALTER ALIAS a.`b`.`c` SET DATABASE TARGET db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("ALTER ALIAS `a`.`b`.c SET DATABASE TARGET db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("ALTER ALIAS a.`b`.c SET DATABASE TARGET db") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("ALTER ALIAS `a`.`b` SET DATABASE TARGET `db.cd`.`ef.gh`.d") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``db.cd`.`ef.gh`.d` for name. Expected name to contain at most two components separated by `.`. (line 1, column 41 (offset: 40))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`db.cd`.`ef.gh`.d' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  private val localAliasClauses = Seq(
    "TARGET db",
    "PROPERTIES { key:'value', anotherKey:'anotherValue' }"
  )

  localAliasClauses.permutations.foreach(clauses => {
    test(s"""ALTER ALIAS name SET DATABASE ${clauses.mkString(" ")}""") {
      assertAst(
        AlterLocalDatabaseAlias(
          namespacedName("name"),
          Some(namespacedName("db")),
          properties =
            Some(Left(Map("key" -> literalString("value"), "anotherKey" -> literalString("anotherValue"))))
        )(defaultPos)
      )
    }
  })

  localAliasClauses.foreach(clause => {
    test(s"""ALTER ALIAS name SET DATABASE $clause $clause""") {
      failsParsing[Statements].withSyntaxErrorContaining(
        s"Duplicate ${clause.substring(0, clause.indexOf(" "))} clause"
      )
    }
  })

  // ALTER REMOTE ALIAS

  test(
    """ALTER ALIAS name SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }"""
  ) {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      Some(namespacedName("target")),
      ifExists = false,
      Some(Left("neo4j://serverA:7687")),
      Some(literalString("user")),
      Some(sensitiveLiteral("password")),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password DRIVER $driver") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("target")),
      ifExists = true,
      Some(Right(stringParam("url"))),
      Some(stringParam("user")),
      Some(pwParam("password")),
      Some(Right(parameter("driver", CTMap)))
    )(defaultPos))
  }

  test("ALTER ALIAS $name SET DATABASE PASSWORD $password USER $user TARGET $target AT $url") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("target")),
      ifExists = false,
      Some(Right(stringParam("url"))),
      Some(stringParam("user")),
      Some(pwParam("password"))
    )(defaultPos))
  }

  test("ALTER ALIAS name.hej SET DATABASE TARGET db AT 'heja'") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          AlterRemoteDatabaseAlias(
            NamespacedName(List("hej"), Some("name"))(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            Some(Left("heja"))
          )(pos)
        )
      case _ => _.toAstPositioned(
          AlterRemoteDatabaseAlias(
            NamespacedName(List("name.hej"), None)(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            Some(Left("heja"))
          )(pos)
        )
    }
  }

  test("""ALTER ALIAS name SET DATABASE USER foo PROPERTIES { key:'value', anotherkey:'anothervalue' }""") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        username = Some(literalString("foo")),
        properties =
          Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anothervalue"))))
      )(defaultPos)
    )
  }

  test(
    """ALTER ALIAS name SET DATABASE USER foo PROPERTIES { key:12.5, anotherkey: { innerKey: 17 }, another: [1,2,'hi'] }"""
  ) {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        username = Some(literalString("foo")),
        properties =
          Some(Left(Map(
            "key" -> literalFloat(12.5),
            "anotherkey" -> mapOf("innerKey" -> literalInt(17)),
            "another" -> listOf(literalInt(1), literalInt(2), literalString("hi"))
          )))
      )(defaultPos)
    )
  }

  test("""ALTER ALIAS name SET DATABASE USER foo PROPERTIES { }""") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        username = Some(literalString("foo")),
        properties =
          Some(Left(Map()))
      )(defaultPos)
    )
  }

  test("""ALTER ALIAS name SET DATABASE USER foo PROPERTIES $props""") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        username = Some(literalString("foo")),
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  private val remoteAliasClauses = Seq(
    "TARGET db AT 'url'",
    "PROPERTIES { key:'value', yetAnotherKey:'yetAnotherValue' }",
    "USER user",
    "PASSWORD 'password'",
    "DRIVER { ssl_enforced: true }",
    "DEFAULT LANGUAGE CYPHER 25"
  )

  remoteAliasClauses.permutations.foreach(clauses => {
    test(s"""ALTER ALIAS name SET DATABASE ${clauses.mkString(" ")}""") {
      assertAst(
        AlterRemoteDatabaseAlias(
          namespacedName("name"),
          Some(namespacedName("db")),
          url = Some(Left("url")),
          username = Some(literalString("user")),
          password = Some(sensitiveLiteral("password")),
          driverSettings = Some(Left(Map("ssl_enforced" -> trueLiteral))),
          properties =
            Some(Left(Map("key" -> literalString("value"), "yetAnotherKey" -> literalString("yetAnotherValue")))),
          defaultLanguage = Some(CypherVersion.Cypher25)
        )(defaultPos)
      )
    }
  })

  remoteAliasClauses.foreach(clause => {
    test(s"""ALTER ALIAS name SET DATABASE $clause $clause""") {
      // 'Default language' contains space so cannot split on space to find the clause name
      val clauseName = if (clause.contains("DEFAULT LANGUAGE")) "DEFAULT LANGUAGE"
      else clause.substring(0, clause.indexOf(' '))
      failsParsing[Statements].withSyntaxErrorContaining(s"Duplicate $clauseName clause")
    }
  })

  test(
    """ALTER ALIAS namespace.name.illegal SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withMessageStart(
          "'.' is not a valid character in the remote alias name 'namespace.name.illegal'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'namespace.name.illegal' for remote alias name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N82,
                "error: data exception - input contains invalid characters. Input 'namespace.name.illegal' contains invalid characters for remote alias name. Special characters may require that the input is quoted using backticks."
              )
          )
      case _ => _.toAstPositioned(
          AlterRemoteDatabaseAlias(
            NamespacedName(List("namespace.name.illegal"), None)(_),
            Some(NamespacedName(List("target"), None)(_)),
            ifExists = false,
            Some(Left("neo4j://serverA:7687")),
            Some("user"),
            Some(sensitiveLiteral("password")),
            Some(Left(Map("ssl_enforced" -> trueLiteral)))
          )(defaultPos)
        )
    }
  }

  // this will instead fail in semantic checking
  test("ALTER ALIAS name SET DATABASE TARGET target DRIVER { ssl_enforced: true }") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      Some(namespacedName("target")),
      ifExists = false,
      None,
      driverSettings = Some(Left(Map("ssl_enforced" -> trueLiteral)))
    )(defaultPos))
  }

  test(
    "ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password TARGET $target DRIVER $driver"
  ) {
    failsParsing[Statements].withSyntaxError(
      """Duplicate TARGET clause (line 1, column 95 (offset: 94))
        |"ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password TARGET $target DRIVER $driver"
        |                                                                                               ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET DATABASE TARGET AT 'url'") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input ''url'': expected a database name, 'AT', 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET', 'USER' or <EOF> (line 1, column 41 (offset: 40))
        |"ALTER ALIAS name SET DATABASE TARGET AT 'url'"
        |                                         ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET DATABASE AT 'url'") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'AT': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 31 (offset: 30))
        |"ALTER ALIAS name SET DATABASE AT 'url'"
        |                               ^""".stripMargin
    )
  }

  test("ALTER ALIAS name.hej.a SET DATABASE TARGET db AT 'heja'") {
    parsesIn[Statements] {
      case Cypher5 => _.withMessageStart(
          "'.' is not a valid character in the remote alias name 'name.hej.a'. " +
            "Remote alias names using '.' must be quoted with backticks " +
            "e.g. `remote.alias`. (line 1, column 13 (offset: 12))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'name.hej.a' for remote alias name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N82,
                "error: data exception - input contains invalid characters. Input 'name.hej.a' contains invalid characters for remote alias name. Special characters may require that the input is quoted using backticks."
              )
          )
      case _ => _.toAstPositioned(
          AlterRemoteDatabaseAlias(
            NamespacedName(List("name.hej.a"), None)(_),
            Some(NamespacedName(List("db"), None)(_)),
            ifExists = false,
            Some(Left("heja"))
          )(defaultPos)
        )
    }
  }

  // set target

  test("""ALTER ALIAS name SET DATABASE TARGET target AT "neo4j://serverA:7687"""") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      targetName = Some(namespacedName("target")),
      url = Some(Left("neo4j://serverA:7687"))
    )(defaultPos))
  }

  test("ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687'") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      targetName = Some(namespacedName("target")),
      url = Some(Left("neo4j://serverA:7687"))
    )(defaultPos))
  }

  test("""ALTER ALIAS name SET DATABASE TARGET target AT """"") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        targetName = Some(namespacedName("target")),
        url = Some(Left(""))
      )(defaultPos)
    )
  }

  test(
    "ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'"
  ) {
    failsParsing[Statements].withSyntaxError(
      """Duplicate TARGET clause (line 1, column 71 (offset: 70))
        |"ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'"
        |                                                                       ^""".stripMargin
    )
  }

  // set user

  test("ALTER ALIAS name SET DATABASE USER user") {
    assertAst(AlterRemoteDatabaseAlias(namespacedName("name"), username = Some(literalString("user")))(defaultPos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE USER $user") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      ifExists = true,
      username = Some(stringParam("user"))
    )(defaultPos))
  }

  test("ALTER ALIAS name SET DATABASE USER $user USER $user") {
    failsParsing[Statements].withSyntaxError(
      """Duplicate USER clause (line 1, column 42 (offset: 41))
        |"ALTER ALIAS name SET DATABASE USER $user USER $user"
        |                                          ^""".stripMargin
    )
  }

  // set password

  test("ALTER ALIAS name SET DATABASE PASSWORD $password") {
    assertAst(
      AlterRemoteDatabaseAlias(namespacedName("name"), password = Some(pwParam("password")))(defaultPos)
    )
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD 'password'") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        ifExists = true,
        password = Some(sensitiveLiteral("password"))
      )(defaultPos)
    )
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD password") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'password': expected a parameter or a string (line 1, column 50 (offset: 49))
        |"ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD password"
        |                                                  ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password") {
    failsParsing[Statements].withSyntaxError(
      """Duplicate PASSWORD clause (line 1, column 50 (offset: 49))
        |"ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password"
        |                                                  ^""".stripMargin
    )
  }

  // set OIDC credential forwarding

  test("ALTER ALIAS name SET DATABASE OIDC CREDENTIAL FORWARDING") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'OIDC': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 31 (offset: 30))
        |"ALTER ALIAS name SET DATABASE OIDC CREDENTIAL FORWARDING"
        |                               ^""".stripMargin
    )
  }

  // set driver

  test("ALTER ALIAS name SET DATABASE DRIVER { ssl_enforced: true }") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      driverSettings = Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  private val alterRemoteDatabaseAliasWithDriverSettings =
    """ALTER ALIAS name SET DATABASE DRIVER
      |{
      |    ssl_enforced: true,
      |    connection_timeout: duration('PT1S'),
      |    connection_max_lifetime: duration('PT1S'),
      |    connection_pool_acquisition_timeout: duration('PT1S'),
      |    connection_pool_idle_test: duration('PT1S'),
      |    connection_pool_max_size: 1000,
      |	   logging_level: "DEBUG"
      |}
      |""".stripMargin

  test(alterRemoteDatabaseAliasWithDriverSettings) {
    val durationExpression = function("duration", literalString("PT1S"))

    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      driverSettings = Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
        "connection_timeout" -> durationExpression,
        "connection_max_lifetime" -> durationExpression,
        "connection_pool_acquisition_timeout" -> durationExpression,
        "connection_pool_idle_test" -> durationExpression,
        "connection_pool_max_size" -> literalInt(1000),
        "logging_level" -> literalString("DEBUG")
      )))
    )(defaultPos))
  }

  test("""ALTER ALIAS name SET DATABASE DRIVER { }""") {
    assertAst(
      AlterRemoteDatabaseAlias(namespacedName("name"), driverSettings = Some(Left(Map.empty)))(defaultPos)
    )
  }

  test("""ALTER ALIAS name SET DATABASE DRIVER $driver DRIVER $driver""") {
    failsParsing[Statements].withSyntaxError(
      """Duplicate DRIVER clause (line 1, column 46 (offset: 45))
        |"ALTER ALIAS name SET DATABASE DRIVER $driver DRIVER $driver"
        |                                              ^""".stripMargin
    )
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'PROPERTY': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 31 (offset: 30))
        |"ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }"
        |                               ^""".stripMargin
    )
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTIES""") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or '{' (line 1, column 41 (offset: 40))
        |"ALTER ALIAS name SET DATABASE PROPERTIES"
        |                                         ^""".stripMargin
    )
  }

  // set default language

  test("""ALTER ALIAS name SET DATABASE DEFAULT LANGUAGE CYPHER 5""") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        defaultLanguage = Some(CypherVersion.Cypher5)
      )(defaultPos)
    )
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE DEFAULT LANGUAGE CYPHER 25") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      ifExists = true,
      defaultLanguage = Some(CypherVersion.Cypher25)
    )(defaultPos))
  }

  test(
    """ALTER ALIAS name SET DATABASE TARGET target AT "url" SET DEFAULT LANGUAGE CYPHER 5"""
  ) {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected 'DEFAULT LANGUAGE CYPHER', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET', 'USER' or <EOF> (line 1, column 54 (offset: 53))
        |"ALTER ALIAS name SET DATABASE TARGET target AT "url" SET DEFAULT LANGUAGE CYPHER 5"
        |                                                      ^""".stripMargin
    )
  }

  test("ALTER ALIAS name SET DATABASE DEFAULT LANGUAGE CYPHER 42") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '42' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '42'. Valid Cypher versions are: 5, 25 (line 1, column 55 (offset: 54))
          |"ALTER ALIAS name SET DATABASE DEFAULT LANGUAGE CYPHER 42"
          |                                                       ^""".stripMargin
      )
  }

  test("ALTER ALIAS name SET DATABASE DEFAULT LANGUAGE CYPHER 42*10") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '42' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '42'. Valid Cypher versions are: 5, 25 (line 1, column 55 (offset: 54))
          |"ALTER ALIAS name SET DATABASE DEFAULT LANGUAGE CYPHER 42*10"
          |                                                       ^""".stripMargin
      )
  }

  // SHOW ALIAS

  test("SHOW ALIASES FOR DATABASE") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(None, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIAS FOR DATABASES") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(None, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIAS db FOR DATABASE") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(Some(namespacedName("db")), None, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES db FOR DATABASE YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(
        Some(namespacedName("db")),
        Some(Left((yieldClause(returnAllItems), None))),
        fromCypher5,
        false
      )(defaultPos)
    )
  }

  test("SHOW ALIAS ns.db FOR DATABASES") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(Some(namespacedName(fromCypher5, "ns", "db")), None, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIAS `ns.db` FOR DATABASE") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(Some(namespacedName("ns.db")), None, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          ShowAliases(Some(namespacedName("ns", "db.db")), None, true, false)(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference 'ns.`db.db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"SHOW ALIAS ns.`db.db` FOR DATABASE"
            |            ^""".stripMargin
        )
    }
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE YIELD * RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          ShowAliases(
            Some(namespacedName("ns", "db.db")),
            Some(Left((yieldClause(returnAllItems), Some(returnAll)))),
            true,
            false
          )(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference 'ns.`db.db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"SHOW ALIAS ns.`db.db` FOR DATABASE YIELD * RETURN *"
            |            ^""".stripMargin
        )
    }
  }

  test("SHOW ALIAS `ns.db`.`db` FOR DATABASE") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          ShowAliases(Some(namespacedName("ns.db", "db")), None, true, false)(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`ns.db`.`db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"SHOW ALIAS `ns.db`.`db` FOR DATABASE"
            |            ^""".stripMargin
        )
    }
  }

  test("SHOW ALIAS `ns.db`.db FOR DATABASE") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          ShowAliases(Some(namespacedName("ns.db", "db")), None, true, false)(defaultPos)
        )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`ns.db`.db'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))
            |"SHOW ALIAS `ns.db`.db FOR DATABASE"
            |            ^""".stripMargin
        )
    }
  }

  test("SHOW ALIASES FOR DATABASE WHERE name = 'alias1'") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(Some(Right(where(equals(varFor("name"), literalString("alias1"))))), fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location") {
    val columns = yieldClause(returnItems(variableReturnItem("location")), None)
    val yieldOrWhere = Some(Left((columns, None)))
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(yieldOrWhere, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val columns = yieldClause(returnItems(variableReturnItem("location")), Some(orderByClause))
    val yieldOrWhere = Some(Left((columns, None)))
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(yieldOrWhere, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database SKIP 1 LIMIT 2 WHERE name = 'alias1' RETURN *") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val whereClause = where(equals(varFor("name"), literalString("alias1")))
    val columns = yieldClause(
      returnItems(variableReturnItem("location")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(2)),
      Some(whereClause)
    )
    val yieldOrWhere = Some(Left((columns, Some(returnAll))))
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(yieldOrWhere, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database OFFSET 1 LIMIT 2 WHERE name = 'alias1' RETURN *") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val whereClause = where(equals(varFor("name"), literalString("alias1")))
    val columns = yieldClause(
      returnItems(variableReturnItem("location")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(2)),
      Some(whereClause)
    )
    val yieldOrWhere = Some(Left((columns, Some(returnAll))))
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(yieldOrWhere, fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      ShowAliases(Some(Left((yieldClause(returnAllItems), None))), fromCypher5, false)(defaultPos)
    )
  }

  test("SHOW ALIASES FOR DATABASE RETURN *") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'RETURN': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 27 (offset: 26))
        |"SHOW ALIASES FOR DATABASE RETURN *"
        |                           ^""".stripMargin
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a variable name or '*' (line 1, column 32 (offset: 31))
        |"SHOW ALIASES FOR DATABASE YIELD"
        |                                ^""".stripMargin
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz)") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '(': expected a variable name or '*' (line 1, column 33 (offset: 32))
        |"SHOW ALIASES FOR DATABASE YIELD (123 + xyz)"
        |                                 ^""".stripMargin
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '(': expected a variable name or '*' (line 1, column 33 (offset: 32))
        |"SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo"
        |                                 ^""".stripMargin
    )
  }

  test("SHOW ALIAS") {
    failsParsing[Statements].withMessage(
      """Invalid input '': expected a database name, a parameter or 'FOR' (line 1, column 11 (offset: 10))
        |"SHOW ALIAS"
        |           ^""".stripMargin
    )
  }

  test("SHOW ALIAS foo, bar FOR DATABASES") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input ',': expected a database name or 'FOR' (line 1, column 15 (offset: 14))
        |"SHOW ALIAS foo, bar FOR DATABASES"
        |               ^""".stripMargin
    )
  }

  test("SHOW ALIAS `a`.`b`.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("SHOW ALIAS `a`.b.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.b.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.b.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.b.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("SHOW ALIAS a.`b`.`c` FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.`c`` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.`c`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("SHOW ALIAS `a`.`b`.c FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``a`.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`a`.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  test("SHOW ALIAS a.`b`.c FOR DATABASE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input `a.`b`.c` for name. Expected name to contain at most two components separated by `.`. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input 'a.`b`.c' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 12 (offset: 11))"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference 'a.`b`.c'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }
}
