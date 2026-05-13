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

import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class AlterDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("READ ONLY", ReadOnlyAccess),
    ("READ WRITE", ReadWriteAccess)
  ).foreach {
    case (accessKeyword, accessType) =>
      test(s"ALTER DATABASE foo SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )(defaultPos)
        )
      }

      test(s"ALTER DATABASE $$foo SET ACCESS $accessKeyword") {
        assertAst(AlterDatabase(
          stringParamName("foo"),
          ifExists = false,
          Some(accessType),
          None,
          NoOptions,
          Set.empty,
          NoWait()(pos),
          None,
          None,
          None
        )(defaultPos))
      }

      test(s"ALTER DATABASE `foo.bar` SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(
            literal("foo.bar"),
            ifExists = false,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )(defaultPos)
        )
      }

      test(s"USE system ALTER DATABASE foo SET ACCESS $accessKeyword") {
        // can parse USE clause, but is not included in AST
        assertAstVersionBased(cypherVersion5 =>
          AlterDatabase(
            literalFoo,
            ifExists = false,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )((1, 12, 11)).withGraph(Some(use(List("system"), !cypherVersion5)))
        )
      }

      test(s"ALTER DATABASE foo IF EXISTS SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(
            literalFoo,
            ifExists = true,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )(defaultPos)
        )
      }
  }

  test("ALTER DATABASE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name or a parameter (line 1, column 15 (offset: 14))
        |"ALTER DATABASE"
        |               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 19 (offset: 18))
        |"ALTER DATABASE foo"
        |                   ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET READ ONLY") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'READ': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET READ ONLY"
            |                        ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'READ': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'PROPERTY', 'ACCESS READ', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET READ ONLY"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo ACCESS READ WRITE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'ACCESS': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 20 (offset: 19))
        |"ALTER DATABASE foo ACCESS READ WRITE"
        |                    ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'ONLY' or 'WRITE' (line 1, column 35 (offset: 34))
        |"ALTER DATABASE foo SET ACCESS READ"
        |                                   ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET ACCESS READWRITE'") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'READWRITE': expected 'READ' (line 1, column 31 (offset: 30))
        |"ALTER DATABASE foo SET ACCESS READWRITE'"
        |                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ_ONLY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'READ_ONLY': expected 'READ' (line 1, column 31 (offset: 30))
        |"ALTER DATABASE foo SET ACCESS READ_ONLY"
        |                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET ACCESS WRITE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'WRITE': expected 'READ' (line 1, column 31 (offset: 30))
        |"ALTER DATABASE foo SET ACCESS WRITE"
        |                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE `foo`.`bar`.`baz` SET ACCESS READ WRITE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``foo`.`bar`.`baz`` for name. Expected name to contain at most two components separated by `.`."
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`foo`.`bar`.`baz`' for name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
              )
          )
      case _ => _.withMessageStart(
          "Incorrectly formatted graph reference '`foo`.`bar`.`baz`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`foo`.`bar`.`baz`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  // Set ACCESS multiple times in the same command
  test("ALTER DATABASE foo SET ACCESS READ ONLY SET ACCESS READ WRITE") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate ACCESS clause (line 1, column 45 (offset: 44))"""
    )
  }

  // Wrong order between IF EXISTS and SET
  test("ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'IF': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
        |"ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS"
        |                                         ^""".stripMargin
    )
  }

  // IF NOT EXISTS instead of IF EXISTS
  test("ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'NOT': expected 'EXISTS' (line 1, column 23 (offset: 22))
        |"ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY"
        |                       ^""".stripMargin
    )
  }

  // ALTER with OPTIONS

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0))))(pos),
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key 1") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("1")(pos)))(pos),
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key -1") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("-1")(pos)))(pos),
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key null") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> Null()(pos)))(pos),
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key1 1 SET OPTION key2 'two'") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            OptionsMap(Map(
              "key1" -> SignedDecimalIntegerLiteral("1")(pos),
              "key2" -> StringLiteral("two")(pos.withInputLength(0))
            ))(defaultPos),
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )(pos)
        )
      case _ => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            OptionsMap(Map(
              "key1" -> SignedDecimalIntegerLiteral("1")(pos),
              "key2" -> StringLiteral("two")(pos.withInputLength(0))
            ))(pos),
            Set.empty,
            NoWait()(pos),
            None,
            None,
            None
          )(pos)
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY SET TOPOLOGY 1 PRIMARY SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadOnlyAccess),
        Some(Topology(Some(Left(1)), None)),
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0))))(pos),
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key2") {
    parsesTo[Statements](
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        NoOptions,
        Set("key", "key2"),
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Left(1)), None)),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY $param PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Right(intParam("param"))), None)),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Left(1)), Some(Right(intParam("param"))))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY $param SECONDARY $param2 PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(Right(intParam("param2"))), Some(Right(intParam("param"))))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ WRITE SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        IndefiniteWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        TimeoutAfter("5")(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SEC") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        TimeoutAfter("5")(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECOND") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        TimeoutAfter("5")(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECONDS") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        TimeoutAfter("5")(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE NOWAIT") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(None, Some(Left(1)))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY $param SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(None, Some(Right(intParam("param"))))),
        NoOptions,
        Set.empty,
        NoWait()(pos),
        None,
        None,
        None
      )(pos)
    )
  }

  // Default language

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 5") {
    parsesTo[Statements](AlterDatabase(
      literalFoo,
      ifExists = false,
      None,
      None,
      NoOptions,
      Set.empty,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher5),
      None,
      None
    )(pos))
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 25") {
    parsesTo[Statements](AlterDatabase(
      literalFoo,
      ifExists = false,
      None,
      None,
      NoOptions,
      Set.empty,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None,
      None
    )(pos))
  }

  test("ALTER DATABASE foo IF EXISTS SET DEFAULT LANGUAGE CYPHER 25") {
    parsesTo[Statements](AlterDatabase(
      literalFoo,
      ifExists = true,
      None,
      None,
      NoOptions,
      Set.empty,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None,
      None
    )(pos))
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 25 SET ACCESS READ WRITE") {
    parsesTo[Statements](AlterDatabase(
      literalFoo,
      ifExists = false,
      Some(ReadWriteAccess),
      None,
      NoOptions,
      Set.empty,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None,
      None
    )(pos))
  }

  test("ALTER DATABASE foo SET OPTION badger 'snake' SET DEFAULT LANGUAGE CYPHER 25 WAIT") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            OptionsMap(Map("badger" -> literalString("snake")))(defaultPos),
            Set.empty,
            IndefiniteWait()(pos),
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
            None,
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            OptionsMap(Map("badger" -> literalString("snake")))(pos),
            Set.empty,
            IndefiniteWait()(pos),
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
            None,
            None
          )(pos)
        )
    }
  }

  test("ALTER DATABASE foo IF EXISTS SET DEFAULT LANGUAGE CYPHER 25 SET TOPOLOGY 1 PRIMARY") {
    parsesTo[Statements](AlterDatabase(
      literalFoo,
      ifExists = true,
      None,
      Some(Topology(Some(Left(1)), None)),
      NoOptions,
      Set.empty,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None,
      None
    )(pos))
  }

  test("ALTER DATABASE foo SET GRAPH SHARD { SET TOPOLOGY 1 PRIMARY }") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET GRAPH SHARD { SET TOPOLOGY 1 PRIMARY }"
            |                        ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            Some(ShardDefinition(0, Some(Topology(Some(Left(1)), None)), None)),
            None
          )(pos)
        )
    }
  }

  test(
    "ALTER DATABASE foo SET GRAPH SHARD { SET TOPOLOGY 2 SECONDARIES } SET PROPERTY SHARD { SET TOPOLOGY 1 REPLICAS }"
  ) {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET GRAPH SHARD { SET TOPOLOGY 2 SECONDARIES } SET PROPERTY SHARD { SET TOPOLOGY 1 REPLICAS }"
            |                        ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            Some(ShardDefinition(0, Some(Topology(None, Some(Left(2)))), Some(Left(1)))),
            None
          )(pos)
        )
    }
  }

  test(
    "ALTER DATABASE foo SET PROPERTY SHARD {SET TOPOLOGY $replica REPLICA} SET DEFAULT LANGUAGE CYPHER 25 SET GRAPH SHARD {SET TOPOLOGY $primary PRIMARY}"
  ) {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PROPERTY': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET PROPERTY SHARD {SET TOPOLOGY $replica REPLICA} SET DEFAULT LANGUAGE CYPHER 25 SET GRAPH SHARD {SET TOPOLOGY $primary PRIMARY}"
            |                        ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
            Some(ShardDefinition(
              0,
              Some(Topology(Some(Right(intParam("primary"))), None)),
              Some(Right(intParam("replica")))
            )),
            None
          )(pos)
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 3 PRIMARIES SET GRAPH SHARD {SET TOPOLOGY $param PRIMARY}") {
    // fails in semantic checking
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 49 (offset: 48))
            |"ALTER DATABASE foo SET TOPOLOGY 3 PRIMARIES SET GRAPH SHARD {SET TOPOLOGY $param PRIMARY}"
            |                                                 ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literalFoo,
            ifExists = false,
            None,
            Some(Topology(Some(Left(3)), None)),
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            Some(ShardDefinition(0, Some(Topology(Some(Right(intParam("param"))), None)), None)),
            None
          )(pos)
        )
    }
  }

  test("ALTER DATABASE `foo-p001` SET TOPOLOGY 2 REPLICAS") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICAS': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 42 (offset: 41))
            |"ALTER DATABASE `foo-p001` SET TOPOLOGY 2 REPLICAS"
            |                                          ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literal("foo-p001"),
            ifExists = false,
            None,
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            Some(Left(2))
          )(pos)
        )
    }
  }

  test("ALTER DATABASE `foo-p001` SET TOPOLOGY $param REPLICAS") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICAS': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 47 (offset: 46))
            |"ALTER DATABASE `foo-p001` SET TOPOLOGY $param REPLICAS"
            |                                               ^""".stripMargin
        )
      case _ => _.toAst(
          AlterDatabase(
            literal("foo-p001"),
            ifExists = false,
            None,
            None,
            NoOptions,
            Set.empty,
            NoWait()(pos),
            None,
            None,
            Some(Right(intParam("param")))
          )(pos)
        )
    }
  }

  // Negative tests

  test("ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'OPTIONS': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 42 (offset: 41))
        |"ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}"
        |                                          ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key") {
    failsParsing[Statements].withSyntaxErrorContaining(
      "Duplicate 'REMOVE OPTION key' clause (line 1, column 52 (offset: 51))",
      GqlStatusInfoCodes.STATUS_42N19,
      "error: syntax error or access rule violation - duplicate clause. Duplicate `'REMOVE OPTION key'` clause."
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'REMOVE': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
        |"ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key"
        |                                         ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET OPTIONS {key: value}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS {key: value}"
            |                        ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'PROPERTY', 'ACCESS READ', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS {key: value}"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION {key: value}") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))
        |"ALTER DATABASE foo SET OPTION {key: value}"
        |                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS key value"
            |                        ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'PROPERTY', 'ACCESS READ', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS key value"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION key value key2 value") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'key2': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
        |"ALTER DATABASE foo SET OPTION key value key2 value"
        |                                         ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET OPTION key value, key2 value") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input ',': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 40 (offset: 39))
        |"ALTER DATABASE foo SET OPTION key value, key2 value"
        |                                        ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key key2") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'key2': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 38 (offset: 37))
        |"ALTER DATABASE foo REMOVE OPTION key key2"
        |                                      ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key, key2") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input ',': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 37 (offset: 36))
        |"ALTER DATABASE foo REMOVE OPTION key, key2"
        |                                     ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTIONS key") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'OPTIONS': expected 'OPTION' (line 1, column 27 (offset: 26))
        |"ALTER DATABASE foo REMOVE OPTIONS key"
        |                           ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements].withSyntaxErrorContaining(
      "Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 58 (offset: 57))",
      GqlStatusInfoCodes.STATUS_42N19,
      "error: syntax error or access rule violation - duplicate clause. Duplicate `'SET OPTION txLogEnrichment'` clause."
    )
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'REMOVE': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 54 (offset: 53))
        |"ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment"
        |                                                      ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 50 (offset: 49))
        |"ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'"
        |                                                  ^""".stripMargin
    )
  }

  // ALTER OR REPLACE
  test("ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OR': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
            |"ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE"
            |       ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OR': expected 'ALIAS', 'CURRENT', 'DATABASE', 'AUTH RULE', 'SERVER', 'USER' or 'USERS' (line 1, column 7 (offset: 6))
            |"ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE"
            |       ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 5 PRIMARIES 10 PRIMARIES 1 PRIMARY 2 SECONDARIES") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES 1 SECONDARIES") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 57 (offset: 56))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 1 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate PRIMARY clause (line 1, column 43 (offset: 42))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 55 (offset: 54))"""
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 REPLICA 2 REPLICAS") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICA': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 35 (offset: 34))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA 2 REPLICAS"
            |                                   ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input '2': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 43 (offset: 42))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA 2 REPLICAS"
            |                                           ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 REPLICAS") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICA': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 35 (offset: 34))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 REPLICAS"
            |                                   ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 REPLICAS"
            |                                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 2 REPLICAS") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICAS': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 58 (offset: 57))
            |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 2 REPLICAS"
            |                                                          ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))
            |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 2 REPLICAS"
            |                                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 PRIMARIES") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'REPLICA': expected 'PRIMARIES', 'PRIMARY', 'SECONDARIES' or 'SECONDARY' (line 1, column 35 (offset: 34))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 PRIMARIES"
            |                                   ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))
            |"ALTER DATABASE foo SET TOPOLOGY 1 REPLICA SET TOPOLOGY 2 PRIMARIES"
            |                                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET GRAPH SHARD {SET TOPOLOGY 1 PRIMARY} SET GRAPH SHARD {SET TOPOLOGY 2 PRIMARIES}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET GRAPH SHARD {SET TOPOLOGY 1 PRIMARY} SET GRAPH SHARD {SET TOPOLOGY 2 PRIMARIES}"
            |                        ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Duplicate GRAPH SHARD clause (line 1, column 65 (offset: 64))
            |"ALTER DATABASE foo SET GRAPH SHARD {SET TOPOLOGY 1 PRIMARY} SET GRAPH SHARD {SET TOPOLOGY 2 PRIMARIES}"
            |                                                                 ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET PROPERTY SHARD {SET TOPOLOGY 1 REPLICA} SET PROPERTY SHARD {SET TOPOLOGY 2 REPLICAS}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PROPERTY': expected 'DEFAULT LANGUAGE CYPHER', 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET PROPERTY SHARD {SET TOPOLOGY 1 REPLICA} SET PROPERTY SHARD {SET TOPOLOGY 2 REPLICAS}"
            |                        ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PROPERTY SHARD clause (line 1, column 68 (offset: 67))
            |"ALTER DATABASE foo SET PROPERTY SHARD {SET TOPOLOGY 1 REPLICA} SET PROPERTY SHARD {SET TOPOLOGY 2 REPLICAS}"
            |                                                                    ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter or an integer value (line 1, column 33 (offset: 32))
        |"ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY"
        |                                 ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter, 'NOWAIT', 'SET', 'WAIT', <EOF> or an integer value (line 1, column 43 (offset: 42))
        |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY"
        |                                           ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter or an integer value (line 1, column 33 (offset: 32))
        |"ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY"
        |                                 ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 45 (offset: 44))""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or an integer value (line 1, column 32 (offset: 31))
        |"ALTER DATABASE foo SET TOPOLOGY"
        |                                ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET DEFAULT") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'LANGUAGE CYPHER' (line 1, column 31 (offset: 30))
        |"ALTER DATABASE foo SET DEFAULT"
        |                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE ") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'CYPHER' (line 1, column 40 (offset: 39))
        |"ALTER DATABASE foo SET DEFAULT LANGUAGE"
        |                                        ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected an integer value (line 1, column 47 (offset: 46))
        |"ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER"
        |                                               ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 22") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '22' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '22'. Valid Cypher versions are: 5, 25 (line 1, column 48 (offset: 47))
          |"ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 22"
          |                                                ^""".stripMargin
      )
  }

  test("ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET DEFAULT LANGUAGE CYPHER 25") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'SET': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 50 (offset: 49))
        |"ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET DEFAULT LANGUAGE CYPHER 25"
        |                                                  ^""".stripMargin
    )
  }

  test("ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 25 REMOVE OPTION txLogEnrichment") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'REMOVE': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 51 (offset: 50))
        |"ALTER DATABASE foo SET DEFAULT LANGUAGE CYPHER 25 REMOVE OPTION txLogEnrichment"
        |                                                   ^""".stripMargin
    )
  }
}
