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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statement

/* Tests for listing functions */
class ShowFunctionsCommandParserTest extends JavaccParserAstTestBase[Statement] {
  implicit val parser: JavaccRule[Statement] = JavaccRule.Statement

  Seq("FUNCTION", "FUNCTIONS").foreach { funcKeyword =>
    Seq(
      ("", ast.AllFunctions),
      ("ALL", ast.AllFunctions),
      ("BUILT IN", ast.BuiltInFunctions),
      ("USER DEFINED", ast.UserDefinedFunctions),
    ).foreach { case (typeString, functionType) =>

      test(s"SHOW $typeString $funcKeyword") {
        yields(_ => query(ast.ShowFunctionsClause(functionType, None, None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE") {
        yields(_ => query(ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT USER") {
        yields(_ => query(ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY user") {
        yields(_ => query(ast.ShowFunctionsClause(functionType, Some(ast.User("user")), None, hasYield = false)(pos)))
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT") {
        yields(_ => query(ast.ShowFunctionsClause(functionType, Some(ast.User("CURRENT")), None, hasYield = false)(pos)))
      }

      test(s"USE db SHOW $typeString $funcKeyword") {
        yields(_ => query(use(varFor("db")), ast.ShowFunctionsClause(functionType, None, None, hasYield = false)(pos)))
      }

    }
  }

  // Filtering tests

  test("SHOW FUNCTION WHERE name = 'my.func'") {
    yields(_ => query(ast.ShowFunctionsClause(ast.AllFunctions, None, Some(where(equals(varFor("name"), literalString("my.func")))), hasYield = false)(pos)))
  }

  test("SHOW FUNCTIONS YIELD description") {
    yields(_ => query(ast.ShowFunctionsClause(ast.AllFunctions, None, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("description")))))
  }

  test("SHOW USER DEFINED FUNCTIONS EXECUTABLE BY user YIELD *") {
    yields(_ => query(ast.ShowFunctionsClause(ast.UserDefinedFunctions, Some(ast.User("user")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW FUNCTIONS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    yields(_ => query(ast.ShowFunctionsClause(ast.AllFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ))
  }

  test("USE db SHOW BUILT IN FUNCTIONS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowFunctionsClause(ast.BuiltInFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("USE db SHOW FUNCTIONS EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowFunctionsClause(ast.AllFunctions, Some(ast.CurrentUser), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW ALL FUNCTIONS YIELD name AS FUNCTION, mode AS OUTPUT") {
    yields(_ => query(ast.ShowFunctionsClause(ast.AllFunctions, None, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "FUNCTION"), aliasedReturnItem("mode", "OUTPUT")))))
  }

  // Negative tests

  test("SHOW FUNCTIONS YIELD") {
    failsToParse
  }

  test("SHOW FUNCTIONS YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' YIELD *") {
    failsToParse
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' RETURN *") {
    failsToParse
  }

  test("SHOW FUNCTIONS YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW FUNCTIONS RETURN *") {
    failsToParse
  }

  test("SHOW EXECUTABLE FUNCTION") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE user") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE CURRENT USER") {
    failsToParse
  }

  test("SHOW FUNCTION EXEC") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY user1, user2") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER user") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER, user") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY user CURRENT USER") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY user, CURRENT USER") {
    failsToParse
  }

  test("SHOW FUNCTION CURRENT USER") {
    failsToParse
  }

  test("SHOW FUNCTION user") {
    failsToParse
  }

  test("SHOW CURRENT USER FUNCTION") {
    failsToParse
  }

  test("SHOW user FUNCTION") {
    failsToParse
  }

  test("SHOW USER user FUNCTION") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE BY USER user") {
    failsToParse
  }

  test("SHOW FUNCTION EXECUTABLE USER user") {
    failsToParse
  }

  test("SHOW FUNCTION USER user") {
    failsToParse
  }

  test("SHOW BUILT FUNCTIONS") {
    failsToParse
  }

  test("SHOW BUILT-IN FUNCTIONS") {
    failsToParse
  }

  test("SHOW USER FUNCTIONS") {
    failsToParse
  }

  test("SHOW USER-DEFINED FUNCTIONS") {
    failsToParse
  }

  test("SHOW FUNCTIONS ALL") {
    failsToParse
  }

  test("SHOW FUNCTIONS BUILT IN") {
    failsToParse
  }

  test("SHOW FUNCTIONS USER DEFINED") {
    failsToParse
  }

  test("SHOW ALL USER DEFINED FUNCTIONS") {
    failsToParse
  }

  test("SHOW ALL BUILT IN FUNCTIONS") {
    failsToParse
  }

  test("SHOW BUILT IN USER DEFINED FUNCTIONS") {
    failsToParse
  }

  test("SHOW USER DEFINED BUILT IN FUNCTIONS") {
    failsToParse
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW FUNCTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW FUNCTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix WITH 'n' as n SHOW FUNCTIONS YIELD name RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS YIELD a WITH a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS YIELD as UNWIND as as a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS YIELD name SHOW FUNCTIONS YIELD name2 RETURN name2") {
      failsToParse
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name2 YIELD name2") {
      failsToParse
    }
  }

  // Brief/verbose not allowed

  test("SHOW FUNCTION BRIEF") {
    failsToParse
  }

  test("SHOW FUNCTION BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW FUNCTIONS BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW FUNCTIONS BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW FUNCTIONS BRIEF WHERE name = 'my.func'") {
    failsToParse
  }

  test("SHOW FUNCTION VERBOSE") {
    failsToParse
  }

  test("SHOW FUNCTION VERBOSE OUTPUT") {
    failsToParse
  }

  test("SHOW FUNCTIONS VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW FUNCTIONS VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW FUNCTIONS VERBOSE WHERE name = 'my.func'") {
    failsToParse
  }

  test("SHOW FUNCTION OUTPUT") {
    failsToParse
  }

}
