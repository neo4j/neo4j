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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class AliasAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  // CREATE ALIAS
  test("CREATE ALIAS alias FOR DATABASE target") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias"), Left("target"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias"), Left("target"), IfExistsDoNothing)(pos))
  }

  test("CREATE OR REPLACE ALIAS alias FOR DATABASE target") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias"), Left("target"), IfExistsReplace)(pos))
  }

  test("CREATE OR REPLACE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias"), Left("target"), IfExistsInvalidSyntax)(pos))
  }

  test("CREATE ALIAS alias.name FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias.name"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS alias . name FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("alias.name"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS IF FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("IF"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS $alias FOR DATABASE $target") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Right(parameter("alias", CTString)), Right(parameter("target", CTString)), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS IF") {
    assertJavaCCException(testName, """Invalid input '': expected ".", "FOR" or "IF" (line 1, column 16 (offset: 15))""")
  }

  test("CREATE ALIAS") {
    assertJavaCCException(testName, """Invalid input '': expected a parameter or an identifier (line 1, column 13 (offset: 12))""")
  }

  test("CREATE ALIAS #Malmö FOR DATABASE db1") {
    assertJavaCCException(testName,
      s"""Invalid input '#': expected a parameter or an identifier (line 1, column 14 (offset: 13))""".stripMargin)
  }

  test("CREATE ALIAS Mal#mö FOR DATABASE db1") {
    assertJavaCCException(testName, s"""Invalid input '#': expected ".", "FOR" or "IF" (line 1, column 17 (offset: 16))""".stripMargin)
  }

  test("CREATE ALIAS `Mal#mö` FOR DATABASE db1") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("Mal#mö"), Left("db1"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS `#Malmö` FOR DATABASE db1") {
    assertJavaCCAST(testName, CreateLocalDatabaseAlias(Left("#Malmö"), Left("db1"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS name FOR DATABASE") {
    assertJavaCCException(testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 31 (offset: 30))""")
  }

  // CREATE REMOTE ALIAS
  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("""CREATE ALIAS name.illegal FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertJavaCCException(testName, "'.' is not a valid character in the remote alias name 'name.illegal'. Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`. (line 1, column 14 (offset: 13))")
  }

  test("""CREATE ALIAS `name.illegal` FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""".stripMargin) {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name.illegal"), Left("target"), IfExistsThrowError, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("CREATE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT '' USER `` PASSWORD ''""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left(""), Left(""), sensitiveLiteral(""))(pos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertJavaCCException(testName, "Invalid input 'neo4j': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 42 (offset: 41))")
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("target", CTString)), IfExistsThrowError,
      Right(parameter("url", CTString)), Right(parameter("user", CTString)), parameter("password", CTString))(pos))
  }

  test("""CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsDoNothing, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("CREATE OR REPLACE ALIAS name FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsReplace, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("CREATE OR REPLACE ALIAS name IF NOT EXISTS FOR DATABASE target AT 'neo4j://serverA:7687' USER user PASSWORD 'password'") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsInvalidSyntax, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"))(pos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
      ))))(pos))
  }

  test("""CREATE ALIAS name IF NOT EXISTS FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password' DRIVER { ssl_enforced: true }""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsDoNothing, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
      ))))(pos))
  }

  test("Create remote database alias with driver settings") {
    val command = """CREATE ALIAS name FOR DATABASE target AT "neo4j://serverA:7687" USER user PASSWORD 'password'
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
    val durationExpression = function("duration", literalString("PT1S"))

    assertJavaCCAST(command, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("neo4j://serverA:7687"), Left("user"), sensitiveLiteral("password"),
      Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
        "connection_timeout" -> durationExpression,
        "connection_max_lifetime" -> durationExpression,
        "connection_pool_acquisition_timeout" -> durationExpression,
        "connection_pool_idle_test" -> durationExpression,
        "connection_pool_max_size" -> literalInt(1000),
        "logging_level" -> literalString("DEBUG")
      ))))(pos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER { foo: 1.0 }""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("bar"), Left("user"), sensitiveLiteral("password"),
      Some(Left(Map(
        "foo" -> literalFloat(1.0),
      ))))(pos))
  }

  test("Should fail to parse CREATE ALIAS with driver settings but no remote url"){
    val command = "CREATE ALIAS name FOR DATABASE target DRIVER { ssl_enforced: true }"
    assertJavaCCException(command, "Invalid input 'DRIVER': expected \".\", \"AT\" or <EOF> (line 1, column 39 (offset: 38))")
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" OPTIONS { foo: 1.0 }""") {
    assertJavaCCException(testName, "Invalid input 'OPTIONS': expected \"USER\" (line 1, column 48 (offset: 47))")
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "bar" USER user PASSWORD "password" DRIVER {}""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("name"), Left("target"), IfExistsThrowError, Left("bar"), Left("user"), sensitiveLiteral("password"),
      Some(Left(Map.empty)))(pos))
  }

  test("""CREATE ALIAS $name FOR DATABASE $target AT $url USER $user PASSWORD $password DRIVER $driver""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("target", CTString)), IfExistsThrowError,
      Right(parameter("url", CTString)), Right(parameter("user", CTString)), parameter("password", CTString ), Some(Right(parameter("driver", CTMap))))(pos))
  }

  test("""CREATE ALIAS driver FOR DATABASE at AT "driver" USER driver PASSWORD "driver" DRIVER {}""") {
    assertJavaCCAST(testName, CreateRemoteDatabaseAlias(Left("driver"), Left("at"), IfExistsThrowError, Left("driver"), Left("driver"), sensitiveLiteral("driver"),
      Some(Left(Map.empty)))(pos))
  }

  test("""CREATE ALIAS name FOR DATABASE target AT "url" USER user PASSWORD "password" DRIVER""") {
    assertJavaCCException(testName, "Invalid input '': expected \"{\" or a parameter (line 1, column 84 (offset: 83))")
  }

  // DROP ALIAS
  test("DROP ALIAS name FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("name"), ifExists = false)(pos))
  }

  test("DROP ALIAS $name FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Right(parameter("name", CTString)), ifExists = false)(pos))
  }

  test("DROP ALIAS name IF EXISTS FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("name"), ifExists = true)(pos))
  }

  test("DROP ALIAS wait FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("wait"), ifExists = false)(pos))
  }

  test("DROP ALIAS nowait FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("nowait"), ifExists = false)(pos))
  }

  test("DROP ALIAS name") {
    assertJavaCCException(testName, "Invalid input '': expected \".\", \"FOR\" or \"IF\" (line 1, column 16 (offset: 15))")
  }

  test("DROP ALIAS name IF EXISTS") {
    assertJavaCCException(testName, "Invalid input '': expected \"FOR\" (line 1, column 26 (offset: 25))")
  }

  // ALTER ALIAS
  test("ALTER ALIAS name SET DATABASE TARGET db") {
    assertJavaCCAST(testName, AlterLocalDatabaseAlias(Left("name"), Left("db"))(pos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE TARGET db") {
    assertJavaCCAST(testName, AlterLocalDatabaseAlias(Left("name"), Left("db"), ifExists = true)(pos))
  }

  test("ALTER ALIAS $name SET DATABASE TARGET $db") {
    assertJavaCCAST(testName, AlterLocalDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("db", CTString)))(pos))
  }

  test("ALTER ALIAS $name if exists SET DATABASE TARGET $db") {
    assertJavaCCAST(testName, AlterLocalDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("db", CTString)), ifExists = true)(pos))
  }

  test("ALTER ALIAS name if exists SET db TARGET") {
    assertJavaCCException(testName, """Invalid input 'db': expected "DATABASE" (line 1, column 32 (offset: 31))""")
  }

  test("ALTER ALIAS name SET TARGET db") {
    assertJavaCCException(testName, """Invalid input 'TARGET': expected "DATABASE" (line 1, column 22 (offset: 21))""")
  }

  test("ALTER DATABASE ALIAS name SET TARGET db") {
    assertJavaCCException(testName, """Invalid input 'name': expected ".", "IF" or "SET" (line 1, column 22 (offset: 21))""")
  }

  test("ALTER RANDOM name") {
    assertJavaCCException(testName, """Invalid input 'RANDOM': expected "ALIAS", "CURRENT", "DATABASE" or "USER" (line 1, column 7 (offset: 6))""")
  }

  // ALTER REMOTE ALIAS
  test("""ALTER ALIAS name SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }""") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), Some(Left("target")), ifExists = false,
      Some(Left("neo4j://serverA:7687")), Some(Left("user")), Some(sensitiveLiteral("password")), Some(Left(Map(
        "ssl_enforced" -> trueLiteral,
      ))))(pos))
  }

  test("""ALTER ALIAS name.illegal SET DATABASE TARGET target AT "neo4j://serverA:7687" USER user PASSWORD "password" DRIVER { ssl_enforced: true }""") {
    assertJavaCCException(testName, ASTExceptionFactory.invalidDotsInRemoteAliasName("name.illegal") + " (line 1, column 13 (offset: 12))")
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password DRIVER $driver") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(
      Right(parameter("name", CTString)),
      Some(Right(parameter("target", CTString))),
      ifExists = true,
      Some(Right(parameter("url", CTString))),
      Some(Right(parameter("user", CTString))),
      Some(parameter("password", CTString)),
      Some(Right(parameter("driver", CTMap))))(pos))
  }

  test("ALTER ALIAS $name SET DATABASE PASSWORD $password USER $user TARGET $target AT $url") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(
      Right(parameter("name", CTString)),
      Some(Right(parameter("target", CTString))),
      ifExists = false,
      Some(Right(parameter("url", CTString))),
      Some(Right(parameter("user", CTString))),
      Some(parameter("password", CTString)))(pos))
  }

  test("Should not fail to parse ALTER ALIAS with driver settings but no remote url") {
    // this will instead fail in semantic checking
    val command = "ALTER ALIAS name SET DATABASE TARGET target DRIVER { ssl_enforced: true }"
    assertJavaCCAST(command, AlterRemoteDatabaseAlias(Left("name"), Some(Left("target")), ifExists = false, None, driverSettings = Some(Left(Map("ssl_enforced" -> trueLiteral))))(pos))
  }

  test("ALTER ALIAS $name IF EXISTS SET DATABASE TARGET $target AT $url USER $user PASSWORD $password TARGET $target DRIVER $driver") {
    assertJavaCCException(testName, "Duplicate SET DATABASE TARGET clause (line 1, column 95 (offset: 94))")
  }

  test("ALTER ALIAS name SET DATABASE TARGET AT 'url'") {
    assertJavaCCExceptionStart(testName, "Invalid input 'url': expected")
  }

  test("ALTER ALIAS name SET DATABASE AT 'url'") {
    assertJavaCCException(testName, """Invalid input 'AT': expected "DRIVER", "PASSWORD", "TARGET" or "USER" (line 1, column 31 (offset: 30))""")
  }

  // set target
  test("""ALTER ALIAS name SET DATABASE TARGET target AT "neo4j://serverA:7687"""") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), targetName = Some(Left("target")), url = Some(Left("neo4j://serverA:7687")))(pos))
  }

  test("ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687'") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), targetName = Some(Left("target")), url = Some(Left("neo4j://serverA:7687")))(pos))
  }

  test("""ALTER ALIAS name SET DATABASE TARGET target AT """"") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), targetName = Some(Left("target")), url = Some(Left("")))(pos))
  }

  test("ALTER ALIAS name SET DATABASE TARGET target AT 'neo4j://serverA:7687' TARGET target AT 'neo4j://serverA:7687'") {
    assertJavaCCException(testName, "Duplicate SET DATABASE TARGET clause (line 1, column 71 (offset: 70))")
  }

  //set user
  test("ALTER ALIAS name SET DATABASE USER user") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), username = Some(Left("user")))(pos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE USER $user") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), ifExists = true, username = Some(Right(parameter("user", CTString))))(pos))
  }

  test("ALTER ALIAS name SET DATABASE USER $user USER $user") {
    assertJavaCCException(testName, "Duplicate SET DATABASE USER clause (line 1, column 42 (offset: 41))")
  }

  //set password
  test("ALTER ALIAS name SET DATABASE PASSWORD $password") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), password = Some(parameter("password", CTString)))(pos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD 'password'") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), ifExists = true, password = Some(sensitiveLiteral("password")))(pos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE PASSWORD password") {
    assertJavaCCException(testName, "Invalid input 'password': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 50 (offset: 49))")
  }

  test("ALTER ALIAS name SET DATABASE PASSWORD $password PASSWORD $password") {
    assertJavaCCException(testName, "Duplicate SET DATABASE PASSWORD clause (line 1, column 50 (offset: 49))")
  }

  //set driver
  test("ALTER ALIAS name SET DATABASE DRIVER { ssl_enforced: true }") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), driverSettings = Some(Left(Map(
      "ssl_enforced" -> trueLiteral,
    ))))(pos))
  }

  test("Alter remote database alias with driver settings") {
    val command = """ALTER ALIAS name SET DATABASE DRIVER
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
    val durationExpression = function("duration", literalString("PT1S"))

    assertJavaCCAST(command, AlterRemoteDatabaseAlias(Left("name"), driverSettings = Some(Left(Map(
            "ssl_enforced" -> trueLiteral,
            "connection_timeout" -> durationExpression,
            "connection_max_lifetime" -> durationExpression,
            "connection_pool_acquisition_timeout" -> durationExpression,
            "connection_pool_idle_test" -> durationExpression,
            "connection_pool_max_size" -> literalInt(1000),
            "logging_level" -> literalString("DEBUG")
          ))))(pos))
  }

  test("""ALTER ALIAS name SET DATABASE DRIVER { }""") {
    assertJavaCCAST(testName, AlterRemoteDatabaseAlias(Left("name"), driverSettings = Some(Left(Map.empty)))(pos))
  }

  test("""ALTER ALIAS name SET DATABASE DRIVER $driver DRIVER $driver""") {
    assertJavaCCException(testName, "Duplicate SET DATABASE DRIVER clause (line 1, column 46 (offset: 45))")
  }

  // SHOW ALIAS

  test("SHOW ALIASES FOR DATABASE") {
    assertJavaCCAST(testName, ShowAliases(None)(pos))
  }

  test("SHOW ALIAS FOR DATABASE") {
    assertJavaCCAST(testName, ShowAliases(None)(pos))
  }

  test("SHOW ALIASES FOR DATABASE WHERE name = 'alias1'") {
    assertJavaCCAST(
      testName,
      ShowAliases(Some(Right(where(equals(varFor("name"), literalString("alias1"))))))(pos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location") {
    val columns = yieldClause(returnItems(variableReturnItem("location")), None)
    val yieldOrWhere = Some(Left((columns, None)))
    assertJavaCCAST(
      testName,
      ShowAliases(yieldOrWhere)(pos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val columns = yieldClause(returnItems(variableReturnItem("location")), Some(orderByClause))
    val yieldOrWhere = Some(Left((columns, None)))
    assertJavaCCAST(
      testName,
      ShowAliases(yieldOrWhere)(pos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD location ORDER BY database SKIP 1 LIMIT 2 WHERE name = 'alias1' RETURN *") {
    val orderByClause = orderBy(sortItem(varFor("database")))
    val whereClause = where(equals(varFor("name"), literalString("alias1")))
    val columns = yieldClause(returnItems(variableReturnItem("location")), Some(orderByClause), Some(skip(1)), Some(limit(2)), Some(whereClause))
    val yieldOrWhere = Some(Left((columns, Some(returnAll))))
    assertJavaCCAST(
      testName,
      ShowAliases(yieldOrWhere)(pos)
    )
  }

  test("SHOW ALIASES FOR DATABASE YIELD *") {
    assertJavaCCAST(
      testName,
      ShowAliases(Some(Left((yieldClause(returnAllItems), None))))(pos)
    )
  }

  test("SHOW ALIASES FOR DATABASE RETURN *") {
    assertJavaCCException(testName, "Invalid input 'RETURN': expected \"WHERE\", \"YIELD\" or <EOF> (line 1, column 27 (offset: 26))")
  }

  test("SHOW ALIASES FOR DATABASE YIELD") {
    assertJavaCCException(testName, "Invalid input '': expected \"*\" or an identifier (line 1, column 32 (offset: 31))")
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz)") {
    assertJavaCCException(testName, "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))")
  }

  test("SHOW ALIASES FOR DATABASE YIELD (123 + xyz) AS foo") {
    assertJavaCCException(testName, "Invalid input '(': expected \"*\" or an identifier (line 1, column 33 (offset: 32))")
  }

  test("SHOW ALIAS") {
    assertJavaCCException(testName, "Invalid input '': expected \"FOR\" (line 1, column 11 (offset: 10))")
  }
}
