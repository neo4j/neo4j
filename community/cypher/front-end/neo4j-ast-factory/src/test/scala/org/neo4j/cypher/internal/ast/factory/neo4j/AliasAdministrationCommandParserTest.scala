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

import org.neo4j.cypher.internal.ast._
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTMap

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
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input '': expected ".", "FOR" or "IF" (line 1, column 16 (offset: 15))"""
    )
  }

  test("CREATE ALIAS") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input '': expected a parameter or an identifier (line 1, column 13 (offset: 12))"""
    )
  }

  test("CREATE ALIAS #Malmö FOR DATABASE db1") {
    assertFailsWithMessage[Statements](
      testName,
      s"""Invalid input '#': expected a parameter or an identifier (line 1, column 14 (offset: 13))""".stripMargin
    )
  }

  test("CREATE ALIAS Mal#mö FOR DATABASE db1") {
    assertFailsWithMessage[Statements](
      testName,
      s"""Invalid input '#': expected ".", "FOR" or "IF" (line 1, column 17 (offset: 16))""".stripMargin
    )
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
    assertFailsWithMessage[Statements](
      testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 31 (offset: 30))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }""") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'PROPERTY': expected ".", "AT", "PROPERTIES" or <EOF> (line 1, column 39 (offset: 38))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTIES""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"{\" or a parameter (line 1, column 49 (offset: 48))"
    )
  }

  // CREATE REMOTE ALIAS
  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      Left("user"),
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
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test(
    """CREATE ALIAS namespace.name.illegal FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    assertFailsWithMessage[Statements](
      testName,
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
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("CREATE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT '' USER `` PASSWORD ''""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left(""),
      Left(""),
      sensitiveLiteral("")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'neo4j': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 42 (offset: 41))"
    )
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      stringParam("url"),
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
      Left("user"),
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
      Left("user"),
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
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'PROPERTIES': expected "USER"""")
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
        Left("user"),
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
        Left("user"),
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
        Left("user"),
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
      Left("user"),
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
      Left("user"),
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
      Left("user"),
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
      Left("user"),
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
      Left("user"),
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
      Left("user"),
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
      Left("user"),
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
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'DRIVER': expected <EOF>""")
  }

  test("Should fail to parse CREATE ALIAS with driver settings but no remote url") {
    val command = "CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }"
    assertFailsWithMessage[Statements](
      command,
      "Invalid input 'DRIVER': expected \".\", \"AT\", \"PROPERTIES\" or <EOF> (line 1, column 39 (offset: 38))"
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'OPTIONS': expected \"USER\" (line 1, column 48 (offset: 47))"
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      namespacedName("name"),
      namespacedName("target"),
      IfExistsThrowError,
      Left("bar"),
      Left("user"),
      sensitiveLiteral("password"),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password DRIVER $driver""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      stringParam("url"),
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
      Left("driver"),
      sensitiveLiteral("driver"),
      Some(Left(Map.empty))
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"{\" or a parameter (line 1, column 84 (offset: 83))"
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }""") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'PROPERTY': expected "DRIVER", "PROPERTIES" or <EOF> (line 1, column 78 (offset: 77))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"{\" or a parameter (line 1, column 88 (offset: 87))"
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
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \".\", \"FOR\" or \"IF\" (line 1, column 16 (offset: 15))"
    )
  }

  test("DROP ALIAS name IF EXISTS") {
    assertFailsWithMessage[Statements](testName, "Invalid input '': expected \"FOR\" (line 1, column 26 (offset: 25))")
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

  test("ALTER ALIAS $name if exists SET DATABASE TARGET $db") {
    assertAst(AlterLocalDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("db")),
      ifExists = true
    )(defaultPos))
  }

  test("ALTER ALIAS name if exists SET db TARGET") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'db': expected "DATABASE" (line 1, column 32 (offset: 31))"""
    )
  }

  test("ALTER ALIAS name SET TARGET db") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'TARGET': expected "DATABASE" (line 1, column 22 (offset: 21))"""
    )
  }

  test("ALTER DATABASE ALIAS name SET TARGET db") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'name': expected ".", "IF", "REMOVE" or "SET" (line 1, column 22 (offset: 21))"""
    )
  }

  test("ALTER ALIAS name SET DATABASE") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input '': expected
        |  "DRIVER"
        |  "PASSWORD"
        |  "PROPERTIES"
        |  "TARGET"
        |  "USER" (line 1, column 30 (offset: 29))""".stripMargin
    )
  }

  test("ALTER RANDOM name") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'RANDOM': expected
        |  "ALIAS"
        |  "CURRENT"
        |  "DATABASE"
        |  "SERVER"
        |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
    )
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
      assertFailsWithMessageStart[Statements](
        testName,
        s"Duplicate SET DATABASE ${clause.substring(0, clause.indexOf(" "))} clause"
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
      Some(Left("user")),
      Some(sensitiveLiteral("password")),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral
      )))
    )(defaultPos))
  }

  test(
    """ALTER ALIAS namespace.name.illegal SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }"""
  ) {
    assertFailsWithMessage[Statements](
      testName,
      ASTExceptionFactory.invalidDotsInRemoteAliasName("namespace.name.illegal") + " (line 1, column 13 (offset: 12))"
    )
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password DRIVER $driver") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("target")),
      ifExists = true,
      Some(stringParam("url")),
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
      Some(stringParam("url")),
      Some(stringParam("user")),
      Some(pwParam("password"))
    )(defaultPos))
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
          username = Some(Left("user")),
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
      assertFailsWithMessageStart[Statements](
        testName,
        s"Duplicate SET DATABASE ${clause.substring(0, clause.indexOf(" "))} clause"
      )
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
    assertFailsWithMessage[Statements](
      testName,
      "Duplicate SET DATABASE TARGET clause (line 1, column 95 (offset: 94))"
    )
  }

  test("ALTER ALIAS name SET DATABASE TARGET AT 'url'") {
    assertFailsWithMessageStart[Statements](testName, "Invalid input 'url': expected")
  }

  test("ALTER ALIAS name SET DATABASE AT 'url'") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'AT': expected
        |  "DRIVER"
        |  "PASSWORD"
        |  "PROPERTIES"
        |  "TARGET"
        |  "USER" (line 1, column 31 (offset: 30))""".stripMargin
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
        username = Some(Left("foo")),
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
        username = Some(Left("foo")),
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
        username = Some(Left("foo")),
        properties =
          Some(Left(Map()))
      )(defaultPos)
    )
  }

  test("""ALTER ALIAS name SET DATABASE USER foo PROPERTIES $props""") {
    assertAst(
      AlterRemoteDatabaseAlias(
        namespacedName("name"),
        username = Some(Left("foo")),
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  test(
    "ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'"
  ) {
    assertFailsWithMessage[Statements](
      testName,
      "Duplicate SET DATABASE TARGET clause (line 1, column 71 (offset: 70))"
    )
  }

  // set user
  test("ALTER ALIAS name SET DATABASE USER user") {
    assertAst(AlterRemoteDatabaseAlias(namespacedName("name"), username = Some(Left("user")))(defaultPos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE USER $user") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      ifExists = true,
      username = Some(stringParam("user"))
    )(defaultPos))
  }

  test("ALTER ALIAS name SET DATABASE USER $user USER $user") {
    assertFailsWithMessage[Statements](testName, "Duplicate SET DATABASE USER clause (line 1, column 42 (offset: 41))")
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
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'password': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 50 (offset: 49))"
    )
  }

  test("ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password") {
    assertFailsWithMessage[Statements](
      testName,
      "Duplicate SET DATABASE PASSWORD clause (line 1, column 50 (offset: 49))"
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
    assertFailsWithMessage[Statements](
      testName,
      "Duplicate SET DATABASE DRIVER clause (line 1, column 46 (offset: 45))"
    )
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }""") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'PROPERTY': expected
        |  "DRIVER"
        |  "PASSWORD"
        |  "PROPERTIES"
        |  "TARGET"
        |  "USER" (line 1, column 31 (offset: 30))""".stripMargin
    )
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTIES""") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"{\" or a parameter (line 1, column 41 (offset: 40))"
    )
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
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'RETURN': expected \"WHERE\", \"YIELD\" or <EOF> (line 1, column 27 (offset: 26))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"*\" or an identifier (line 1, column 32 (offset: 31))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz)") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
    )
  }

  test("SHOW ALIAS") {
    assertFailsWithMessage[Statements](testName, "Invalid input '': expected \"FOR\" (line 1, column 11 (offset: 10))")
  }

  test("SHOW ALIAS foo, bar FOR DATABASES") {
    assertFailsWithMessageStart[Statements](testName, "Invalid input 'foo': expected \"FOR\"")
  }
}
