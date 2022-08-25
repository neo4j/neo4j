/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString

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
        NamespacedName(List("name"), Some("alias"))(_),
        NamespacedName(List("name"), Some("db"))(_),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias . name FOR DATABASE db.name") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName(List("name"), Some("alias"))(_),
        NamespacedName(List("name"), Some("db"))(_),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS IF FOR DATABASE db.name") {
    assertAst(CreateLocalDatabaseAlias(
      NamespacedName("IF")(_),
      NamespacedName(List("name"), Some("db"))(_),
      IfExistsThrowError
    )(
      defaultPos
    ))
  }

  test("CREATE ALIAS composite.alias FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName(List("alias"), Some("composite"))(_),
        NamespacedName("db")(_),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.alias FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName(List("alias"), Some("alias"))(_),
        NamespacedName("db")(_),
        IfExistsThrowError
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.if IF NOT EXISTS FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName(List("if"), Some("alias"))(_),
        NamespacedName("db")(_),
        IfExistsDoNothing
      )(
        defaultPos
      )
    )
  }

  test("CREATE ALIAS alias.for FOR DATABASE db") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName(List("for"), Some("alias"))(_),
        NamespacedName("db")(_),
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
      NamespacedName("alias")(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anotherValue"))))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target PROPERTIES { key:12.5, anotherkey: { innerKey: 17 }, another: [1,2,'hi'] }"""
  ) {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
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
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
        IfExistsThrowError,
        properties =
          Some(Left(Map()))
      )(defaultPos)
    )
  }

  test("""CREATE ALIAS alias FOR DATABASE target PROPERTIES $props""") {
    assertAst(
      CreateLocalDatabaseAlias(
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
        IfExistsThrowError,
        properties =
          Some(Right(parameter("props", CTMap)))
      )(defaultPos)
    )
  }

  test("CREATE ALIAS IF") {
    assertFailsWithMessage(
      testName,
      """Invalid input '': expected ".", "FOR" or "IF" (line 1, column 16 (offset: 15))"""
    )
  }

  test("CREATE ALIAS") {
    assertFailsWithMessage(
      testName,
      """Invalid input '': expected a parameter or an identifier (line 1, column 13 (offset: 12))"""
    )
  }

  test("CREATE ALIAS #Malmö FOR DATABASE db1") {
    assertFailsWithMessage(
      testName,
      s"""Invalid input '#': expected a parameter or an identifier (line 1, column 14 (offset: 13))""".stripMargin
    )
  }

  test("CREATE ALIAS Mal#mö FOR DATABASE db1") {
    assertFailsWithMessage(
      testName,
      s"""Invalid input '#': expected ".", "FOR" or "IF" (line 1, column 17 (offset: 16))""".stripMargin
    )
  }

  test("CREATE ALIAS `Mal#mö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(NamespacedName("Mal#mö")(_), NamespacedName("db1")(_), IfExistsThrowError)(
      defaultPos
    ))
  }

  test("CREATE ALIAS `#Malmö` FOR DATABASE db1") {
    assertAst(CreateLocalDatabaseAlias(NamespacedName("#Malmö")(_), NamespacedName("db1")(_), IfExistsThrowError)(
      defaultPos
    ))
  }

  test("CREATE ALIAS name FOR DATABASE") {
    assertFailsWithMessage(
      testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 31 (offset: 30))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTY { key: 'val' }""") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'PROPERTY': expected ".", "AT", "PROPERTIES" or <EOF> (line 1, column 39 (offset: 38))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target PROPERTIES""") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"{\" or a parameter (line 1, column 49 (offset: 48))")
  }

  // CREATE REMOTE ALIAS
  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName(List("name.illegal"), Some("namespace"))(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test(
    """CREATE ALIAS namespace.name.illegal FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    assertFailsWithMessage(
      testName,
      "'.' is not a valid character in the remote alias name 'namespace.name.illegal'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`. (line 1, column 14 (offset: 13))"
    )
  }

  test(
    """CREATE ALIAS `name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""".stripMargin
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name.illegal")(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("CREATE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name")(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Left("neo4j://serverA:7687"),
      Left("user"),
      sensitiveLiteral("password")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT '' USER `` PASSWORD ''""") {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name")(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Left(""),
      Left(""),
      sensitiveLiteral("")
    )(defaultPos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'neo4j': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 42 (offset: 41))"
    )
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password""") {
    assertAst(CreateRemoteDatabaseAlias(
      stringParamName("name"),
      stringParamName("target"),
      IfExistsThrowError,
      Right(parameter("url", CTString)),
      Right(parameter("user", CTString)),
      parameter("password", CTString)
    )(defaultPos))
  }

  test(
    """CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'"""
  ) {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName(List("name"), Some("composite"))(_),
      NamespacedName("target")(_),
      IfExistsDoNothing,
      Left("neo4j://serverA:7687"),
      Left("user"),
      sensitiveLiteral("password"),
      None,
      Some(Left(Map("key" -> literalString("value"), "anotherkey" -> literalString("anotherValue"))))
    )(defaultPos))
  }

  test(
    """CREATE ALIAS alias FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
      |PROPERTIES { key:12.5, anotherkey: { innerKey: 17 }, another: [1,2,'hi'] }""".stripMargin
  ) {
    assertAst(
      CreateRemoteDatabaseAlias(
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
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
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
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
        NamespacedName("alias")(_),
        NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      NamespacedName("name")(_),
      NamespacedName("target")(_),
      IfExistsThrowError,
      Left("bar"),
      Left("user"),
      sensitiveLiteral("password"),
      Some(Left(Map(
        "foo" -> literalFloat(1.0)
      )))
    )(defaultPos))
  }

  test("Should fail to parse CREATE ALIAS with driver settings but no remote url") {
    val command = "CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }"
    assertFailsWithMessage(
      command,
      "Invalid input 'DRIVER': expected \".\", \"AT\", \"PROPERTIES\" or <EOF> (line 1, column 39 (offset: 38))"
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }""") {
    assertFailsWithMessage(testName, "Invalid input 'OPTIONS': expected \"USER\" (line 1, column 48 (offset: 47))")
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER {}""") {
    assertAst(CreateRemoteDatabaseAlias(
      NamespacedName("name")(_),
      NamespacedName("target")(_),
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
      Right(parameter("url", CTString)),
      Right(parameter("user", CTString)),
      parameter("password", CTString),
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
    assertFailsWithMessage(testName, "Invalid input '': expected \"{\" or a parameter (line 1, column 84 (offset: 83))")
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTY { key: 'val' }""") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'PROPERTY': expected "DRIVER", "PROPERTIES" or <EOF> (line 1, column 78 (offset: 77))"""
    )
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" PROPERTIES""") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"{\" or a parameter (line 1, column 88 (offset: 87))")
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
    assertAst(DropDatabaseAlias(NamespacedName(List("name"), Some("composite"))(_), ifExists = false)(defaultPos))
  }

  test("DROP ALIAS composite.`dotted.name` FOR DATABASE") {
    assertAst(
      DropDatabaseAlias(NamespacedName(List("dotted.name"), Some("composite"))(_), ifExists = false)(defaultPos)
    )
  }

  test("DROP ALIAS `dotted.composite`.name FOR DATABASE") {
    assertAst(
      DropDatabaseAlias(NamespacedName(List("name"), Some("dotted.composite"))(_), ifExists = false)(defaultPos)
    )
  }

  test("DROP ALIAS name") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \".\", \"FOR\" or \"IF\" (line 1, column 16 (offset: 15))"
    )
  }

  test("DROP ALIAS name IF EXISTS") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"FOR\" (line 1, column 26 (offset: 25))")
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
    assertFailsWithMessage(testName, """Invalid input 'db': expected "DATABASE" (line 1, column 32 (offset: 31))""")
  }

  test("ALTER ALIAS name SET TARGET db") {
    assertFailsWithMessage(testName, """Invalid input 'TARGET': expected "DATABASE" (line 1, column 22 (offset: 21))""")
  }

  test("ALTER DATABASE ALIAS name SET TARGET db") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'name': expected ".", "IF" or "SET" (line 1, column 22 (offset: 21))"""
    )
  }

  test("ALTER ALIAS name SET DATABASE") {
    assertFailsWithMessage(
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
    assertFailsWithMessage(
      testName,
      """Invalid input 'RANDOM': expected "ALIAS", "CURRENT", "DATABASE" or "USER" (line 1, column 7 (offset: 6))"""
    )
  }

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
    assertFailsWithMessage(
      testName,
      ASTExceptionFactory.invalidDotsInRemoteAliasName("namespace.name.illegal") + " (line 1, column 13 (offset: 12))"
    )
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password DRIVER $driver") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("target")),
      ifExists = true,
      Some(Right(parameter("url", CTString))),
      Some(Right(parameter("user", CTString))),
      Some(parameter("password", CTString)),
      Some(Right(parameter("driver", CTMap)))
    )(defaultPos))
  }

  test("ALTER ALIAS $name SET DATABASE PASSWORD $password USER $user TARGET $target AT $url") {
    assertAst(AlterRemoteDatabaseAlias(
      stringParamName("name"),
      Some(stringParamName("target")),
      ifExists = false,
      Some(Right(parameter("url", CTString))),
      Some(Right(parameter("user", CTString))),
      Some(parameter("password", CTString))
    )(defaultPos))
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
    assertFailsWithMessage(testName, "Duplicate SET DATABASE TARGET clause (line 1, column 95 (offset: 94))")
  }

  test("ALTER ALIAS name SET DATABASE TARGET AT 'url'") {
    assertFailsWithMessageStart(testName, "Invalid input 'url': expected")
  }

  test("ALTER ALIAS name SET DATABASE AT 'url'") {
    assertFailsWithMessage(
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
    assertFailsWithMessage(testName, "Duplicate SET DATABASE TARGET clause (line 1, column 71 (offset: 70))")
  }

  // set user
  test("ALTER ALIAS name SET DATABASE USER user") {
    assertAst(AlterRemoteDatabaseAlias(namespacedName("name"), username = Some(Left("user")))(defaultPos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE USER $user") {
    assertAst(AlterRemoteDatabaseAlias(
      namespacedName("name"),
      ifExists = true,
      username = Some(Right(parameter("user", CTString)))
    )(defaultPos))
  }

  test("ALTER ALIAS name SET DATABASE USER $user USER $user") {
    assertFailsWithMessage(testName, "Duplicate SET DATABASE USER clause (line 1, column 42 (offset: 41))")
  }

  // set password
  test("ALTER ALIAS name SET DATABASE PASSWORD $password") {
    assertAst(
      AlterRemoteDatabaseAlias(namespacedName("name"), password = Some(parameter("password", CTString)))(
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
    assertFailsWithMessage(
      testName,
      "Invalid input 'password': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 50 (offset: 49))"
    )
  }

  test("ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password") {
    assertFailsWithMessage(testName, "Duplicate SET DATABASE PASSWORD clause (line 1, column 50 (offset: 49))")
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
    assertFailsWithMessage(testName, "Duplicate SET DATABASE DRIVER clause (line 1, column 46 (offset: 45))")
  }

  test("""ALTER ALIAS name SET DATABASE PROPERTY { key: 'val' }""") {
    assertFailsWithMessage(
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
    assertFailsWithMessage(testName, "Invalid input '': expected \"{\" or a parameter (line 1, column 41 (offset: 40))")
  }

  // SHOW ALIAS

  test("SHOW ALIASES FOR DATABASE") {
    assertAst(ShowAliases(None)(defaultPos))
  }

  test("SHOW ALIAS FOR DATABASE") {
    assertAst(ShowAliases(None)(defaultPos))
  }

  test("SHOW ALIAS db FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("db")), None)(defaultPos))
  }

  test("SHOW ALIAS db FOR DATABASE YIELD *") {
    assertAst(
      ShowAliases(Some(namespacedName("db")), Some(Left((yieldClause(returnAllItems), None))))(defaultPos)
    )
  }

  test("SHOW ALIAS ns.db FOR DATABASE") {
    assertAst(ShowAliases(Some(NamespacedName(List("db"), Some("ns"))(pos)), None)(defaultPos))
  }

  test("SHOW ALIAS `ns.db` FOR DATABASE") {
    assertAst(ShowAliases(Some(namespacedName("ns.db")), None)(defaultPos))
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE") {
    assertAst(ShowAliases(Some(NamespacedName(List("db.db"), Some("ns"))(pos)), None)(defaultPos))
  }

  test("SHOW ALIAS ns.`db.db` FOR DATABASE YIELD * RETURN *") {
    assertAst(ShowAliases(
      Some(NamespacedName(List("db.db"), Some("ns"))(pos)),
      Some(Left((yieldClause(returnAllItems), Some(returnAll))))
    )(defaultPos))
  }

  test("SHOW ALIAS `ns.db`.`db` FOR DATABASE") {
    assertAst(ShowAliases(Some(NamespacedName(List("db"), Some("ns.db"))(pos)), None)(defaultPos))
  }

  test("SHOW ALIAS `ns.db`.db FOR DATABASE") {
    assertAst(ShowAliases(Some(NamespacedName(List("db"), Some("ns.db"))(pos)), None)(defaultPos))
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
    assertFailsWithMessage(
      testName,
      "Invalid input 'RETURN': expected \"WHERE\", \"YIELD\" or <EOF> (line 1, column 27 (offset: 26))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \"*\" or an identifier (line 1, column 32 (offset: 31))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz)") {
    assertFailsWithMessage(
      testName,
      "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo") {
    assertFailsWithMessage(
      testName,
      "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))"
    )
  }

  test("SHOW ALIAS") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"FOR\" (line 1, column 11 (offset: 10))")
  }
}
