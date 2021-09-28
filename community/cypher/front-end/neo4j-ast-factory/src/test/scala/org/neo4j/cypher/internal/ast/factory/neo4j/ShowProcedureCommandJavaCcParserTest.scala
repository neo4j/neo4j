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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ShowProcedureCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  Seq("PROCEDURE", "PROCEDURES").foreach { procKeyword =>

    test(s"SHOW $procKeyword") {
      assertJavaCCAST(testName, query(ShowProceduresClause(None, None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE") {
      assertJavaCCAST(testName, query(ShowProceduresClause(Some(CurrentUser), None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT USER") {
      assertJavaCCAST(testName, query(ShowProceduresClause(Some(CurrentUser), None, hasYield = false)(pos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY user") {
      assertJavaCCAST(testName, query(ShowProceduresClause(Some(User("user")), None, hasYield = false)(pos)))
    }

    test(s"USE db SHOW $procKeyword") {
      assertJavaCCAST(testName, query(use(varFor("db")), ShowProceduresClause(None, None, hasYield = false)(pos)))
    }

  }

  // Filtering tests

  test("SHOW PROCEDURE WHERE name = 'my.proc'") {
    assertJavaCCAST(testName, query(ShowProceduresClause(None, Some(where(equals(varFor("name"), literalString("my.proc")))), hasYield = false)(pos)))
  }

  test("SHOW PROCEDURES YIELD description") {
    assertJavaCCAST(testName, query(ShowProceduresClause(None, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("description")))))
  }

  test("SHOW PROCEDURES EXECUTABLE BY user YIELD *") {
    assertJavaCCAST(testName, query(ShowProceduresClause(Some(User("user")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW PROCEDURES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName, query(ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ))
  }

  test("USE db SHOW PROCEDURES YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("USE db SHOW PROCEDURES EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowProceduresClause(Some(CurrentUser), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW PROCEDURES YIELD name AS PROCEDURE, mode AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowProceduresClause(None, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "PROCEDURE"), aliasedReturnItem("mode", "OUTPUT")))))
  }

  // Negative tests

  test("SHOW PROCEDURES YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES YIELD") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES YIELD * YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES YIELD a b RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW EXECUTABLE PROCEDURE") {
    assertJavaCCException(testName,
      """Invalid input 'EXECUTABLE': expected
        |  "ALL"
        |  "BTREE"
        |  "BUILT"
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "CURRENT"
        |  "DATABASE"
        |  "DATABASES"
        |  "DEFAULT"
        |  "EXIST"
        |  "EXISTENCE"
        |  "EXISTS"
        |  "FULLTEXT"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "HOME"
        |  "INDEX"
        |  "INDEXES"
        |  "LOOKUP"
        |  "NODE"
        |  "POINT"
        |  "POPULATED"
        |  "PROCEDURE"
        |  "PROCEDURES"
        |  "PROPERTY"
        |  "RANGE"
        |  "REL"
        |  "RELATIONSHIP"
        |  "ROLES"
        |  "TEXT"
        |  "UNIQUE"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW PROCEDURE EXECUTABLE user") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXEC") {
    assertJavaCCException(testName, """Invalid input 'EXEC': expected "EXECUTABLE", "WHERE", "YIELD" or <EOF> (line 1, column 16 (offset: 15))""")
  }

  test("SHOW PROCEDURE EXECUTABLE BY") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE BY user1, user2") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER user") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER, user") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE BY user CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE BY user, CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE user") {
    assertSameAST(testName)
  }

  test("SHOW CURRENT USER PROCEDURE") {
    assertSameAST(testName)
  }

  test("SHOW user PROCEDURE") {
    assertJavaCCException(testName, """Invalid input 'PROCEDURE': expected "DEFINED" (line 1, column 11 (offset: 10))""")
  }

  test("SHOW USER user PROCEDURE") {
    assertJavaCCException(testName, """Invalid input 'user': expected "DEFINED" (line 1, column 11 (offset: 10))""")
  }

  test("SHOW PROCEDURE EXECUTABLE BY USER user") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE EXECUTABLE USER user") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE USER user") {
    assertSameAST(testName)
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW PROCEDURES YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW PROCEDURES YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW PROCEDURES YIELD name RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD a WITH a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD as UNWIND as as a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD name SHOW PROCEDURES YIELD name2 RETURN name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES RETURN name2 YIELD name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW PROCEDURE BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE BRIEF OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES BRIEF YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES BRIEF RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES BRIEF WHERE name = 'my.proc'") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE VERBOSE OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES VERBOSE YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES VERBOSE RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURES VERBOSE WHERE name = 'my.proc'") {
    assertSameAST(testName)
  }

  test("SHOW PROCEDURE OUTPUT") {
    assertSameAST(testName)
  }

}
