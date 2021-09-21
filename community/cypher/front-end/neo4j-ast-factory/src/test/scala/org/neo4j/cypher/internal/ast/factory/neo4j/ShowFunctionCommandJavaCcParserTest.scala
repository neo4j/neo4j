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

import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ShowFunctionCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  Seq("FUNCTION", "FUNCTIONS").foreach { funcKeyword =>
    Seq(
      ("", AllFunctions),
      ("ALL", AllFunctions),
      ("BUILT IN", BuiltInFunctions),
      ("USER DEFINED", UserDefinedFunctions),
    ).foreach { case (typeString, functionType) =>

      test(s"SHOW $typeString $funcKeyword") {
        assertJavaCCAST(testName, query(ShowFunctionsClause(functionType, None, None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE") {
        assertJavaCCAST(testName, query(ShowFunctionsClause(functionType, Some(CurrentUser), None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT USER") {
        assertJavaCCAST(testName, query(ShowFunctionsClause(functionType, Some(CurrentUser), None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY user") {
        assertJavaCCAST(testName, query(ShowFunctionsClause(functionType, Some(User("user")), None, hasYield = false)(pos)))
      }

      test(s"USE db SHOW $typeString $funcKeyword") {
        assertJavaCCAST(testName, query(use(varFor("db")), ShowFunctionsClause(functionType, None, None, hasYield = false)(pos)))
      }

    }
  }

  // Filtering tests

  test("SHOW FUNCTION WHERE name = 'my.func'") {
    assertJavaCCAST(testName, query(ShowFunctionsClause(AllFunctions, None, Some(where(equals(varFor("name"), literalString("my.func")))), hasYield = false)(pos)))
  }

  test("SHOW FUNCTIONS YIELD description") {
    assertJavaCCAST(testName, query(ShowFunctionsClause(AllFunctions, None, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("description")))))
  }

  test("SHOW USER DEFINED FUNCTIONS EXECUTABLE BY user YIELD *") {
    assertJavaCCAST(testName, query(ShowFunctionsClause(UserDefinedFunctions, Some(User("user")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW FUNCTIONS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName, query(ShowFunctionsClause(AllFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ))
  }

  test("USE db SHOW BUILT IN FUNCTIONS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowFunctionsClause(BuiltInFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("USE db SHOW FUNCTIONS EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowFunctionsClause(AllFunctions, Some(CurrentUser), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW ALL FUNCTIONS YIELD name AS FUNCTION, mode AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowFunctionsClause(AllFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "FUNCTION"), aliasedReturnItem("mode", "OUTPUT")))))
  }

  // Negative tests

  test("SHOW FUNCTIONS YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS YIELD") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS YIELD * YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS YIELD a b RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW EXECUTABLE FUNCTION") {
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
        |  "TRANSACTION"
        |  "TRANSACTIONS"
        |  "UNIQUE"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW FUNCTION EXECUTABLE user") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXEC") {
    assertJavaCCException(testName, """Invalid input 'EXEC': expected "EXECUTABLE", "WHERE", "YIELD" or <EOF> (line 1, column 15 (offset: 14))""")
  }

  test("SHOW FUNCTION EXECUTABLE BY") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE BY user1, user2") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER user") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER, user") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE BY user CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE BY user, CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION user") {
    assertSameAST(testName)
  }

  test("SHOW CURRENT USER FUNCTION") {
    assertSameAST(testName)
  }

  test("SHOW user FUNCTION") {
    assertJavaCCException(testName, """Invalid input 'FUNCTION': expected "DEFINED" (line 1, column 11 (offset: 10))""")
  }

  test("SHOW USER user FUNCTION") {
    assertJavaCCException(testName, """Invalid input 'user': expected "DEFINED" (line 1, column 11 (offset: 10))""")
  }

  test("SHOW FUNCTION EXECUTABLE BY USER user") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION EXECUTABLE USER user") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION USER user") {
    assertSameAST(testName)
  }

  test("SHOW BUILT FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW BUILT-IN FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW USER FUNCTIONS") {
    assertJavaCCException(testName, """Invalid input 'FUNCTIONS': expected "DEFINED" (line 1, column 11 (offset: 10))""")
  }

  test("SHOW USER-DEFINED FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS ALL") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS BUILT IN") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS USER DEFINED") {
    assertSameAST(testName)
  }

  test("SHOW ALL USER DEFINED FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW ALL BUILT IN FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW BUILT IN USER DEFINED FUNCTIONS") {
    assertSameAST(testName)
  }

  test("SHOW USER DEFINED BUILT IN FUNCTIONS") {
    assertSameAST(testName)
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW FUNCTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW FUNCTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW FUNCTIONS YIELD name RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS YIELD a WITH a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS YIELD as UNWIND as as a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW FUNCTIONS YIELD name SHOW FUNCTIONS YIELD name2 RETURN name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name2 YIELD name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW FUNCTION BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION BRIEF OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS BRIEF YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS BRIEF RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS BRIEF WHERE name = 'my.func'") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION VERBOSE OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS VERBOSE YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS VERBOSE RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTIONS VERBOSE WHERE name = 'my.func'") {
    assertSameAST(testName)
  }

  test("SHOW FUNCTION OUTPUT") {
    assertSameAST(testName)
  }

}
