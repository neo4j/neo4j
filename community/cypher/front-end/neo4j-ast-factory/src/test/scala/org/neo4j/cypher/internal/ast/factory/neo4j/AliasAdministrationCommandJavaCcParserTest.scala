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

import org.neo4j.cypher.internal.ast.AlterDatabaseAlias
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CreateDatabaseAlias
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class AliasAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  // CREATE ALIAS

  test("CREATE ALIAS alias FOR DATABASE target") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias"), Left("target"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias"), Left("target"), IfExistsDoNothing)(pos))
  }

  test("CREATE OR REPLACE ALIAS alias FOR DATABASE target") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias"), Left("target"), IfExistsReplace)(pos))
  }

  test("CREATE OR REPLACE ALIAS alias IF NOT EXISTS FOR DATABASE target") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias"), Left("target"), IfExistsInvalidSyntax)(pos))
  }

  test("CREATE ALIAS alias.name FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias.name"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS alias . name FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("alias.name"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS IF FOR DATABASE db.name") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("IF"), Left("db.name"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS $alias FOR DATABASE $target") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Right(parameter("alias", CTString)), Right(parameter("target", CTString)), IfExistsThrowError)(pos))
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
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("Mal#mö"), Left("db1"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS `#Malmö` FOR DATABASE db1") {
    assertJavaCCAST(testName, CreateDatabaseAlias(Left("#Malmö"), Left("db1"), IfExistsThrowError)(pos))
  }

  test("CREATE ALIAS name FOR DATABASE") {
    assertJavaCCException(testName,
      s"""Invalid input '': expected a parameter or an identifier (line 1, column 31 (offset: 30))""")
  }

  // DROP ALIAS
  test("DROP ALIAS name FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("name"), false)(pos))
  }

  test("DROP ALIAS $name FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Right(parameter("name", CTString)), false)(pos))
  }

  test("DROP ALIAS name IF EXISTS FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("name"), true)(pos))
  }

  test("DROP ALIAS wait FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("wait"), false)(pos))
  }

  test("DROP ALIAS nowait FOR DATABASE") {
    assertJavaCCAST(testName, DropDatabaseAlias(Left("nowait"), false)(pos))
  }

  // ALTER ALIAS
  test("ALTER ALIAS name SET DATABASE TARGET db") {
    assertJavaCCAST(testName, AlterDatabaseAlias(Left("name"), Left("db"), false)(pos))
  }

  test("ALTER ALIAS name IF EXISTS SET DATABASE TARGET db") {
    assertJavaCCAST(testName, AlterDatabaseAlias(Left("name"), Left("db"), true)(pos))
  }

  test("ALTER ALIAS $name SET DATABASE TARGET $db") {
    assertJavaCCAST(testName, AlterDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("db", CTString)), false)(pos))
  }

  test("ALTER ALIAS $name if exists SET DATABASE TARGET $db") {
    assertJavaCCAST(testName, AlterDatabaseAlias(Right(parameter("name", CTString)), Right(parameter("db", CTString)), true)(pos))
  }

  test("ALTER ALIAS name if exists SET db TARGET") {
    assertJavaCCException(testName, """Invalid input 'db': expected "DATABASE" (line 1, column 32 (offset: 31))""")
  }

  test("ALTER DATABASE ALIAS name SET TARGET db if exists") {
    assertJavaCCException(testName, """Invalid input 'name': expected ".", "IF" or "SET" (line 1, column 22 (offset: 21))""")
  }

  test("ALTER FUNCTION name SET TARGET db if exists") {
    assertJavaCCException(testName, """Invalid input 'FUNCTION': expected "ALIAS", "CURRENT", "DATABASE" or "USER" (line 1, column 7 (offset: 6))""")
  }

  test("ALTER FUNCTION name SET TARGET db if not exists") {
    assertJavaCCException(testName, """Invalid input 'FUNCTION': expected "ALIAS", "CURRENT", "DATABASE" or "USER" (line 1, column 7 (offset: 6))""")
  }
}
