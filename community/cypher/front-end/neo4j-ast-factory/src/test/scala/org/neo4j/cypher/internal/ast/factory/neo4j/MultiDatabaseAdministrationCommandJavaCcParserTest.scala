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

import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class MultiDatabaseAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {
  // SHOW DATABASE

  Seq(
    "DATABASES",
    "DEFAULT DATABASE",
    "DATABASE $db",
    "DATABASE neo4j"
  ).foreach { dbType =>

    test(s"SHOW $dbType") {
      assertSameAST(testName)
    }

    test(s"USE SYSTEM SHOW $dbType") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED'") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' AND action = 'match'") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD access ORDER BY access") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD access ORDER BY access WHERE access ='none'") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD access SKIP -1") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD access ORDER BY access RETURN access") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType WHERE access = 'GRANTED' RETURN action") {
      assertSameAST(testName)
    }

    test(s"SHOW $dbType YIELD * RETURN *") {
      assertSameAST(testName)
    }
  }

  test("SHOW DATABASE `foo.bar`") {
    assertSameAST(testName)
  }

  test("SHOW DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("SHOW DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))")
  }

  test("SHOW DATABASE blah YIELD *,database, databases, default, dbms RETURN user") {
    assertSameAST(testName)
  }

  // CREATE DATABASE

  test("CREATE DATABASE foo") {
    assertSameAST(testName)
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    assertSameAST(testName)
  }

  test("CREATE DATABASE $wait") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE `nowait.sec`") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE second WAIT") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE seconds WAIT 12") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE dump WAIT 12 SEC") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE destroy WAIT 12 SECOND") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE data WAIT 12 SECONDS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE `foo.bar`") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE `graph.db`.`db.db`") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE `foo-bar42`") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE `_foo-bar42`") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE ``") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT 10 SECONDS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT") {
    assertSameAST(testName)
  }

  test("CREATE  DATABASE foo IF NOT EXISTS NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE foo") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE foo WAIT 10 SECONDS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE foo WAIT") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE foo NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE OR REPLACE DATABASE `_foo-bar42`") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE \"foo.bar\"") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE foo-bar42") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE _foo-bar42") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE 42foo-bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 24 (offset: 23))")
  }

  test("CATALOG CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    val exceptionMessage =
      s"""Invalid input 'NOT': expected
         |  "."
         |  "IF"
         |  "NOWAIT"
         |  "WAIT"
         |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("CREATE DATABASE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo WAIT -12") {
    assertSameAST(testName)
  }

  test("CREATE DATABASE foo WAIT 3.14") {
    assertJavaCCException(testName, "Invalid input '3.14': expected <EOF> or <UNSIGNED_DECIMAL_INTEGER> (line 1, column 26 (offset: 25))")
  }

  test("CREATE DATABASE foo WAIT bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE OR REPLACE DATABASE _foo-bar42") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))")
  }

  // DROP DATABASE

  test("DROP DATABASE foo") {
    assertSameAST(testName)
  }

  test("DROP DATABASE $foo") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo WAIT") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo WAIT 10") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo WAIT 10 SEC") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo WAIT 10 SECOND") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo WAIT 10 SECONDS") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG DROP DATABASE `foo.bar`") {
    assertSameAST(testName)
  }

  test("CATALOG DROP DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS WAIT") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS NOWAIT") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo DUMP DATA") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo DESTROY DATA") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS DUMP DATA") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA WAIT") {
    assertSameAST(testName)
  }

  test("DROP DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))")
  }

  test("DROP DATABASE  IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP DATABASE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("DROP DATABASE KEEP DATA") {
    val exceptionMessage =
      s"""Invalid input 'DATA': expected
         |  "."
         |  "DESTROY"
         |  "DUMP"
         |  "IF"
         |  "NOWAIT"
         |  "WAIT"
         |  <EOF> (line 1, column 20 (offset: 19))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  // START DATABASE

  test("START DATABASE start") {
    assertSameAST(testName)
  }

  test("START DATABASE $foo") {
    assertSameAST(testName)
  }

  test("START DATABASE foo WAIT") {
    assertSameAST(testName)
  }

  test("START DATABASE foo WAIT 5") {
    assertSameAST(testName)
  }

  test("START DATABASE foo WAIT 5 SEC") {
    assertSameAST(testName)
  }

  test("START DATABASE foo WAIT 5 SECOND") {
    assertSameAST(testName)
  }

  test("START DATABASE foo WAIT 5 SECONDS") {
    assertSameAST(testName)
  }

  test("START DATABASE foo NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG START DATABASE `foo.bar`") {
    assertSameAST(testName)
  }

  test("CATALOG START DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("START DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))")
  }

  // STOP DATABASE

  test("STOP DATABASE stop") {
    assertSameAST(testName)
  }

  test("STOP DATABASE $foo") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo WAIT") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo WAIT 99") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo WAIT 99 SEC") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo WAIT 99 SECOND") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo WAIT 99 SECONDS") {
    assertSameAST(testName)
  }

  test("STOP DATABASE foo NOWAIT") {
    assertSameAST(testName)
  }

  test("CATALOG STOP DATABASE `foo.bar`") {
    assertSameAST(testName)
  }

  test("CATALOG STOP DATABASE foo.bar") {
    assertSameAST(testName)
  }

  test("STOP DATABASE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))")
  }
}
