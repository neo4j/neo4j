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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.exceptions.SyntaxException

class AliasAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
    assertAst(CreateLocalDatabaseAlias(namespacedName("alias"), namespacedName("target"), IfExistsReplace)(
      defaultPos
    ))
  }

  test("CREATE OR REPLACE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertAst(
      CreateLocalDatabaseAlias(namespacedName("alias"), namespacedName("target"), IfExistsInvalidSyntax)(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.name FOR DATABASE db.name") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias", "name"),
        namespacedName("db", "name"),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias . name FOR DATABASE db.name") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias", "name"),
        namespacedName("db", "name"),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS IF FOR DATABASE db.name") {
    assertAst(CreateLocalDatabaseAlias(
      namespacedName("IF"),
      namespacedName("db", "name"),
      IfExistsThrowError
    )(
      defaultPos
    ))
  }

  test("CREATE ALIAS composite.alias FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("composite", "alias"),
        namespacedName("db"),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.alias FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias", "alias"),
        namespacedName("db"),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.if IF NOT EXISTS FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias", "if"),
        namespacedName("db"),
        IfExistsDoNothing
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.for FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        namespacedName("alias", "for"),
        namespacedName("db"),
        IfExistsThrowError
      )(
        defaultPos
      )
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

  test("CREATE ALIAS IF") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        """Invalid input '': expected ".", "FOR" or "IF" (line 1, column 16 (offset: 15))"""
      ))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '': expected a database name, 'FOR DATABASE' or 'IF NOT EXISTS' (line 1, column 16 (offset: 15))
          |"CREATE ALIAS IF"
          |                ^""".stripMargin
      ))
  }

  test("CREATE ALIAS") {
    // TODO Accept worse error?
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '': expected a parameter or an identifier (line 1, column 13 (offset: 12))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name, a graph pattern or a parameter (line 1, column 13 (offset: 12))
          |"CREATE ALIAS"
          |             ^""".stripMargin
      ))
  }

  test("CREATE ALIAS #Malmö FOR DATABASE db1") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '#': expected a parameter or an identifier (line 1, column 14 (offset: 13))""".stripMargin
      ))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '#': expected a database name, a graph pattern or a parameter (line 1, column 14 (offset: 13))
          |"CREATE ALIAS #Malmö FOR DATABASE db1"
          |              ^""".stripMargin
      ))
  }

  test("CREATE ALIAS Mal#mö FOR DATABASE db1") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '#': expected ".", "FOR" or "IF" (line 1, column 17 (offset: 16))"""
      ))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '#': expected a database name, 'FOR DATABASE' or 'IF NOT EXISTS' (line 1, column 17 (offset: 16))
          |"CREATE ALIAS Mal#mö FOR DATABASE db1"
          |                 ^""".stripMargin
      ))
  }

  test("CREATE ALIAS `Mal#mö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(namespacedName("Mal#mö"), namespacedName("db1"), IfExistsThrowError)(
      defaultPos
    ))
  }

  test("CREATE ALIAS `#Malmö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(namespacedName("#Malmö"), namespacedName("db1"), IfExistsThrowError)(
      defaultPos
    ))
  }

  test("CREATE ALIAS name FOR DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        s"""Invalid input '': expected a parameter or an identifier (line 1, column 31 (offset: 30))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name or a parameter (line 1, column 31 (offset: 30))
          |"CREATE ALIAS name FOR DATABASE"
          |                               ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'PROPERTY': expected ".", "AT", "PROPERTIES" or <EOF> (line 1, column 39 (offset: 38))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'PROPERTY': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
          |"CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }"
          |                                       ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTIES""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"{\" or a parameter (line 1, column 49 (offset: 48))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a parameter or '{' (line 1, column 49 (offset: 48))
          |"CREATE ALIAS name FOR DATABASE target PROPERTIES"
          |                                                 ^""".stripMargin
      ))
  }

  // CREATE REMOTE ALIAS
  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test(
    """CREATE ALIAS namespace.`name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("namespace", "name.illegal"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test(
    """CREATE ALIAS namespace.name.illegal FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    failsParsing[Statements].withMessageStart(
      "'.' is not a valid character in the remote alias name 'namespace.name.illegal'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`. (line 1, column 14 (offset: 13))"
    )
  }

  test(
    """CREATE ALIAS `name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""".stripMargin
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name.illegal"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("CREATE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT '' USER `` PASSWORD ''""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left(""),
      literalString(""),
      sensitiveLiteral("")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'neo4j': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 42 (offset: 41))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'neo4j': expected a parameter or a string (line 1, column 42 (offset: 41))
          |"CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'"
          |                                          ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      Right(stringParam("url")),
      stringParam("user"),
      pwParam("password")
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
      literalString("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test(
    """CREATE ALIAS composite.name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |PROPERTIES { key:'value', anotherkey:'anotherValue' }""".stripMargin
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("composite", "name"),
      namespacedName("target"),
      IfExistsDoNothing,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password"),
      None,
      Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anotherValue"))))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS composite.name FOR DATABASE target AT "neo4j://serverA:7687"
      |PROPERTIES { key:'value', anotherkey:'anotherValue' }
      |USER user PASSWORD 'password'""".stripMargin
  ) {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'PROPERTIES': expected "USER""""))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'PROPERTIES': expected 'USER' (line 2, column 1 (offset: 74))
          |"PROPERTIES { key:'value', anotherkey:'anotherValue' }"
          | ^""".stripMargin
      ))
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
        literalString("user"),
        sensitiveLiteral("password"),
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
        literalString("user"),
        sensitiveLiteral("password"),
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
        literalString("user"),
        sensitiveLiteral("password"),
        None,
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  test("CREATE OR REPLACE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsReplace,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password")
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
      literalString("user"),
      sensitiveLiteral("password")
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
      literalString("user"),
      sensitiveLiteral("password"),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password' DRIVER { ssl_enforced: true }"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsDoNothing,
      Left("neo4j://serverA:7687"),
      literalString("user"),
      sensitiveLiteral("password"),
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
      literalString("user"),
      sensitiveLiteral("password"),
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
      literalString("user"),
      sensitiveLiteral("password"),
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
      literalString("user"),
      sensitiveLiteral("password"),
      Some(Left(Map(
        "foo" -> literalFloat(1.0)
      ))),
      Some(Left(Map("bar" -> trueLiteral)))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" PROPERTIES { bar: true } DRIVER { foo: 1.0 }"""
  ) {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'DRIVER': expected <EOF>"""))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DRIVER': expected <EOF> (line 1, column 103 (offset: 102))
          |"CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" PROPERTIES { bar: true } DRIVER { foo: 1.0 }"
          |                                                                                                       ^""".stripMargin
      ))
  }

  test("Should fail to parse CREATE ALIAS with driver settings but no remote url") {
    "CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }" should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'DRIVER': expected \".\", \"AT\", \"PROPERTIES\" or <EOF> (line 1, column 39 (offset: 38))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'DRIVER': expected a database name, 'AT', 'PROPERTIES' or <EOF> (line 1, column 39 (offset: 38))
          |"CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }"
          |                                       ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'OPTIONS': expected \"USER\" (line 1, column 48 (offset: 47))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'OPTIONS': expected 'USER' (line 1, column 48 (offset: 47))
          |"CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }"
          |                                                ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("bar"),
      literalString("user"),
      sensitiveLiteral("password"),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password DRIVER $driver""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      Right(stringParam("url")),
      stringParam("user"),
      pwParam("password"),
      Some(Right(parameter("driver", CTMap)))
    )(defaultPos))
  }

  test("""CREATE ALIAS driver FOR DATABASE at AT "driver" USER driver PASSWORD "driver" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("driver"),
      namespacedName("at"),
      IfExistsThrowError,
      Left("driver"),
      literalString("driver"),
      sensitiveLiteral("driver"),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"{\" or a parameter (line 1, column 84 (offset: 83))"
      ))
      // TODO Message good enough?
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a parameter or '{' (line 1, column 84 (offset: 83))
          |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER"
          |                                                                                    ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'PROPERTY': expected "DRIVER", "PROPERTIES" or <EOF> (line 1, column 78 (offset: 77))"""
      ))
      // TODO Message good enough?
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'PROPERTY': expected 'DRIVER', 'PROPERTIES' or <EOF> (line 1, column 78 (offset: 77))
          |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }"
          |                                                                              ^""".stripMargin
      ))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"{\" or a parameter (line 1, column 88 (offset: 87))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a parameter or '{' (line 1, column 88 (offset: 87))
          |"CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES"
          |                                                                                        ^""".stripMargin
      ))
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
    assertAst(DropDatabaseAlias(namespacedName("composite", "name"), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS composite.`dotted.name` FOR DATABASE") {
    assertAst(
      DropDatabaseAlias(namespacedName("composite", "dotted.name"), ifExists = false)(defaultPos)
    )
  }

  test("DROP ALIAS `dotted.composite`.name FOR DATABASE") {
    assertAst(
      DropDatabaseAlias(namespacedName("dotted.composite", "name"), ifExists = false)(defaultPos)
    )
  }

  test("DROP ALIAS name") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \".\", \"FOR\" or \"IF\" (line 1, column 16 (offset: 15))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a database name, 'FOR DATABASE' or 'IF EXISTS' (line 1, column 16 (offset: 15))
          |"DROP ALIAS name"
          |                ^""".stripMargin
      ))
  }

  test("DROP ALIAS name IF EXISTS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"FOR\" (line 1, column 26 (offset: 25))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected 'FOR DATABASE' (line 1, column 26 (offset: 25))
          |"DROP ALIAS name IF EXISTS"
          |                          ^""".stripMargin
      ))
  }

  // ALTER ALIAS
  test("ALTER ALIAS name SET DATABASE TARGET db") {
    assertAst(AlterLocalDatabaseAlias(namespacedName("name"), Some(namespacedName("db")))(defaultPos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE TARGET db") {
    assertAst(AlterLocalDatabaseAlias(namespacedName("name"), Some(namespacedName("db")), ifExists = true)(
      defaultPos
    ))
  }

  test("ALTER ALIAS $name SET DATABASE TARGET $db") {
    assertAst(
      AlterLocalDatabaseAlias(stringParamName("name"), Some(stringParamName("db")))(defaultPos)
    )
  }

  test("ALTER ALIAS name.hej SET DATABASE TARGET db") {
    parsesTo[Statements](Statements(Seq(AlterLocalDatabaseAlias(
      namespacedName("name", "hej"),
      Some(namespacedName("db")),
      ifExists = false,
      None
    )(pos))))
  }

  test("ALTER ALIAS name.hej.a SET DATABASE TARGET db") {
    parsesTo[Statements](Statements(Seq(AlterLocalDatabaseAlias(
      namespacedName("name", "hej", "a"),
      Some(namespacedName("db")),
      ifExists = false,
      None
    )(pos))))
  }

  test("ALTER ALIAS $name if exists SET DATABASE TARGET $db") {
    assertAst(AlterLocalDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("db")),
      ifExists = true
    )(defaultPos))
  }

  test("ALTER ALIAS name if exists SET db TARGET") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'db': expected "DATABASE" (line 1, column 32 (offset: 31))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'db': expected 'DATABASE' (line 1, column 32 (offset: 31))
          |"ALTER ALIAS name if exists SET db TARGET"
          |                                ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name SET TARGET db") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'TARGET': expected "DATABASE" (line 1, column 22 (offset: 21))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'TARGET': expected 'DATABASE' (line 1, column 22 (offset: 21))
          |"ALTER ALIAS name SET TARGET db"
          |                      ^""".stripMargin
      ))
  }

  test("ALTER DATABASE ALIAS name SET TARGET db") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'name': expected ".", "IF", "REMOVE" or "SET" (line 1, column 22 (offset: 21))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'name': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 22 (offset: 21))
          |"ALTER DATABASE ALIAS name SET TARGET db"
          |                      ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name SET DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input '': expected
          |  "DRIVER"
          |  "PASSWORD"
          |  "PROPERTIES"
          |  "TARGET"
          |  "USER" (line 1, column 30 (offset: 29))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 30 (offset: 29))
          |"ALTER ALIAS name SET DATABASE"
          |                              ^""".stripMargin
      ))
  }

  test("ALTER RANDOM name") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'RANDOM': expected
          |  "ALIAS"
          |  "CURRENT"
          |  "DATABASE"
          |  "SERVER"
          |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'RANDOM': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
          |"ALTER RANDOM name"
          |       ^""".stripMargin
      ))
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
      failsParsing[Statements]
        .parseIn(JavaCc)(_.withMessageStart(
          s"Duplicate SET DATABASE ${clause.substring(0, clause.indexOf(" "))} clause"
        ))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          s"Duplicate ${clause.substring(0, clause.indexOf(" "))} clause"
        ))
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

  test(
    """ALTER ALIAS namespace.name.illegal SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }"""
  ) {
    failsParsing[Statements].withMessageStart(
      ASTExceptionFactory.invalidDotsInRemoteAliasName("namespace.name.illegal") + " (line 1, column 13 (offset: 12))"
    )
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
    parsesTo[Statements](Statements(Seq(AlterRemoteDatabaseAlias(
      namespacedName("name", "hej"),
      Some(namespacedName("db")),
      ifExists = false,
      Some(Left("heja"))
    )(pos))))
  }

  private val remoteAliasClauses = Seq(
    "TARGET db AT 'url'",
    "PROPERTIES { key:'value', yetAnotherKey:'yetAnotherValue' }",
    "USER user",
    "PASSWORD 'password'",
    "DRIVER { ssl_enforced: true }"
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
            Some(Left(Map("key" -> literalString("value"), "yetAnotherKey" -> literalString("yetAnotherValue"))))
        )(defaultPos)
      )
    }
  })

  remoteAliasClauses.foreach(clause => {
    test(s"""ALTER ALIAS name SET DATABASE $clause $clause""") {
      failsParsing[Statements]
        .parseIn(JavaCc)(_.withMessageStart(
          s"Duplicate SET DATABASE ${clause.substring(0, clause.indexOf(" "))} clause"
        ))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          s"Duplicate ${clause.substring(0, clause.indexOf(" "))} clause"
        ))
    }
  })

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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Duplicate SET DATABASE TARGET clause (line 1, column 95 (offset: 94))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate TARGET clause (line 1, column 95 (offset: 94))
          |"ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password TARGET $target DRIVER $driver"
          |                                                                                               ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name SET DATABASE TARGET AT 'url'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'url': expected"
      ))
      // TODO Error ok?
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input ''url'': expected a database name, 'AT', 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET', 'USER' or <EOF> (line 1, column 41 (offset: 40))
          |"ALTER ALIAS name SET DATABASE TARGET AT 'url'"
          |                                         ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name SET DATABASE AT 'url'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'AT': expected
          |  "DRIVER"
          |  "PASSWORD"
          |  "PROPERTIES"
          |  "TARGET"
          |  "USER" (line 1, column 31 (offset: 30))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'AT': expected 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 31 (offset: 30))
          |"ALTER ALIAS name SET DATABASE AT 'url'"
          |                               ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name.hej.a SET DATABASE TARGET db AT 'heja'") {
    failsParsing[Statements].withMessageStart(
      "'.' is not a valid character in the remote alias name 'name.hej.a'. " +
        "Remote alias names using '.' must be quoted with backticks " +
        "e.g. `remote.alias`. (line 1, column 13 (offset: 12))"
    )
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
      )(
        defaultPos
      )
    )
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

  test(
    "ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'"
  ) {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Duplicate SET DATABASE TARGET clause (line 1, column 71 (offset: 70))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate TARGET clause (line 1, column 71 (offset: 70))
          |"ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'"
          |                                                                       ^""".stripMargin
      ))
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Duplicate SET DATABASE USER clause (line 1, column 42 (offset: 41))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate USER clause (line 1, column 42 (offset: 41))
          |"ALTER ALIAS name SET DATABASE USER $user USER $user"
          |                                          ^""".stripMargin
      ))
  }

  // set password
  test("ALTER ALIAS name SET DATABASE PASSWORD $password") {
    assertAst(
      AlterRemoteDatabaseAlias(namespacedName("name"), password = Some(pwParam("password")))(
        defaultPos
      )
    )
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD 'password'") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        ifExists = true,
        password = Some(sensitiveLiteral("password"))
      )(
        defaultPos
      )
    )
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD password") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'password': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 50 (offset: 49))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'password': expected a parameter or a string (line 1, column 50 (offset: 49))
          |"ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD password"
          |                                                  ^""".stripMargin
      ))
  }

  test("ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Duplicate SET DATABASE PASSWORD clause (line 1, column 50 (offset: 49))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate PASSWORD clause (line 1, column 50 (offset: 49))
          |"ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password"
          |                                                  ^""".stripMargin
      ))
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

  private val alterRemoteDatabaseAliasWithDriverSettings = """ALTER ALIAS name SET DATABASE DRIVER
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
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Duplicate SET DATABASE DRIVER clause (line 1, column 46 (offset: 45))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate DRIVER clause (line 1, column 46 (offset: 45))
          |"ALTER ALIAS name SET DATABASE DRIVER $driver DRIVER $driver"
          |                                              ^""".stripMargin
      ))
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'PROPERTY': expected
          |  "DRIVER"
          |  "PASSWORD"
          |  "PROPERTIES"
          |  "TARGET"
          |  "USER" (line 1, column 31 (offset: 30))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'PROPERTY': expected 'DRIVER', 'PASSWORD', 'PROPERTIES', 'TARGET' or 'USER' (line 1, column 31 (offset: 30))
          |"ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }"
          |                               ^""".stripMargin
      ))
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTIES""") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"{\" or a parameter (line 1, column 41 (offset: 40))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a parameter or '{' (line 1, column 41 (offset: 40))
          |"ALTER ALIAS name SET DATABASE PROPERTIES"
          |                                         ^""".stripMargin
      ))
  }

  // SHOW ALIAS

  test("SHOW ALIASES FOR DATABASE") {
    assertAst(ShowAliases(None)(defaultPos))
  }

  test("SHOW ALIAS FOR DATABASES") {
    assertAst(ShowAliases(None)(defaultPos))
  }

  test("SHOW ALIAS db FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("db")), None)(defaultPos))
  }

  test("SHOW ALIASES db FOR DATABASE YIELD *") {
    assertAst(
      ShowAliases(Some(namespacedName("db")), Some(Left((yieldClause(returnAllItems), None))))(defaultPos)
    )
  }

  test("SHOW ALIAS ns.db FOR DATABASES") {
    assertAst(ShowAliases(Some(namespacedName("ns", "db")), None)(defaultPos))
  }

  test("SHOW ALIAS `ns.db` FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("ns.db")), None)(defaultPos))
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("ns", "db.db")), None)(defaultPos))
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE YIELD * RETURN *") {
    assertAst(ShowAliases(
      Some(namespacedName("ns", "db.db")),
      Some(Left((yieldClause(returnAllItems), Some(returnAll))))
    )(defaultPos))
  }

  test("SHOW ALIAS `ns.db`.`db` FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("ns.db", "db")), None)(defaultPos))
  }

  test("SHOW ALIAS `ns.db`.db FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("ns.db", "db")), None)(defaultPos))
  }

  test("SHOW ALIASES FOR DATABASE WHERE name = 'alias1'") {
    assertAst(ShowAliases(Some(Right(where(equals(varFor("name"), literalString("alias1"))))))(defaultPos))
  }

  test("SHOW ALIASES FOR DATABASE YIELD location") {
    val columns = yieldClause(returnItems(variableReturnItem("location")), None)
    val yieldOrWhere = Some(Left((columns, None)))
    assertAst(ShowAliases(yieldOrWhere)(defaultPos))
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val columns = yieldClause(returnItems(variableReturnItem("location")), Some(orderByClause))
    val yieldOrWhere = Some(Left((columns, None)))
    assertAst(ShowAliases(yieldOrWhere)(defaultPos))
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
    assertAst(ShowAliases(yieldOrWhere)(defaultPos))
  }

  test("SHOW ALIASES FOR DATABASE YIELD *") {
    assertAst(ShowAliases(Some(Left((yieldClause(returnAllItems), None))))(defaultPos))
  }

  test("SHOW ALIASES FOR DATABASE RETURN *") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'RETURN': expected \"WHERE\", \"YIELD\" or <EOF> (line 1, column 27 (offset: 26))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'RETURN': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 27 (offset: 26))
          |"SHOW ALIASES FOR DATABASE RETURN *"
          |                           ^""".stripMargin
      ))
  }

  test("SHOW ALIASES FOR DATABASE YIELD") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"*\" or an identifier (line 1, column 32 (offset: 31))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a variable name or '*' (line 1, column 32 (offset: 31))
          |"SHOW ALIASES FOR DATABASE YIELD"
          |                                ^""".stripMargin
      ))
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz)") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '(': expected a variable name or '*' (line 1, column 33 (offset: 32))
          |"SHOW ALIASES FOR DATABASE YIELD (123 + xyz)"
          |                                 ^""".stripMargin
      ))
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '(': expected a variable name or '*' (line 1, column 33 (offset: 32))
          |"SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo"
          |                                 ^""".stripMargin
      ))
  }

  test("SHOW ALIAS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '': expected \"FOR\" (line 1, column 11 (offset: 10))"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '': expected a database name, a parameter or 'FOR' (line 1, column 11 (offset: 10))
          |"SHOW ALIAS"
          |           ^""".stripMargin
      ))
  }

  test("SHOW ALIAS foo, bar FOR DATABASES") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'foo': expected \"FOR\""))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input ',': expected a database name or 'FOR' (line 1, column 15 (offset: 14))
          |"SHOW ALIAS foo, bar FOR DATABASES"
          |               ^""".stripMargin
      ))
  }
}
