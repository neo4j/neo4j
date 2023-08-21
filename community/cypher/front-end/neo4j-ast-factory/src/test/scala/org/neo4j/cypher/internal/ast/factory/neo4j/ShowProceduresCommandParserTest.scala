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

import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.StringLiteral

/* Tests for listing procedures */
class ShowProceduresCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("PROCEDURE", "PROCEDURES").foreach { procKeyword =>
    test(s"SHOW $procKeyword") {
      assertAst(singleQuery(ShowProceduresClause(None, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE") {
      assertAst(singleQuery(ShowProceduresClause(Some(CurrentUser), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT USER") {
      assertAst(singleQuery(ShowProceduresClause(Some(CurrentUser), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY user") {
      assertAst(singleQuery(ShowProceduresClause(Some(User("user")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT") {
      assertAst(singleQuery(ShowProceduresClause(Some(User("CURRENT")), None, hasYield = false)(defaultPos)))
    }

    test(s"USE db SHOW $procKeyword") {
      assertAst(SingleQuery(
        List(use(varFor("db", (1, 5, 4))), ShowProceduresClause(None, None, hasYield = false)((1, 8, 7)))
      )((1, 8, 7)))
    }

  }

  // Filtering tests

  test("SHOW PROCEDURE WHERE name = 'my.proc'") {
    assertAst(singleQuery(ShowProceduresClause(
      None,
      Some(Where(
        Equals(
          varFor("name", (1, 22, 21)),
          StringLiteral("my.proc")((1, 29, 28))
        )((1, 27, 26))
      )((1, 16, 15))),
      hasYield = false
    )(defaultPos)))
  }

  test("SHOW PROCEDURES YIELD description") {
    assertAst(singleQuery(
      ShowProceduresClause(None, None, hasYield = true)(defaultPos),
      yieldClause(
        ReturnItems(includeExisting = false, Seq(variableReturnItem("description", (1, 23, 22))))((1, 23, 22))
      )
    ))
  }

  test("SHOW PROCEDURES EXECUTABLE BY user YIELD *") {
    assertAst(singleQuery(
      ShowProceduresClause(Some(User("user")), None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems)
    ))
  }

  test("SHOW PROCEDURES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(singleQuery(
      ShowProceduresClause(None, None, hasYield = true)(defaultPos),
      yieldClause(
        returnAllItems((1, 25, 24)),
        Some(OrderBy(Seq(
          AscSortItem(varFor("name", (1, 34, 33)))((1, 34, 33))
        ))((1, 25, 24))),
        Some(skip(2, (1, 39, 38))),
        Some(limit(5, (1, 46, 45)))
      )
    ))
  }

  test("USE db SHOW PROCEDURES YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertAst(
      singleQuery(
        use(varFor("db")),
        ShowProceduresClause(None, None, hasYield = true)(pos),
        yieldClause(
          returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
          where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))
        ),
        return_(variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW PROCEDURES EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAst(
      singleQuery(
        use(varFor("db")),
        ShowProceduresClause(Some(CurrentUser), None, hasYield = true)(pos),
        yieldClause(
          returnItems(variableReturnItem("name"), aliasedReturnItem("description", "pp")),
          Some(orderBy(sortItem(varFor("pp")))),
          Some(skip(2)),
          Some(limit(5)),
          Some(where(lessThan(varFor("pp"), literalFloat(50.0))))
        ),
        return_(variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test("SHOW PROCEDURES YIELD name AS PROCEDURE, mode AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowProceduresClause(None, None, hasYield = true)(pos),
        yieldClause(returnItems(aliasedReturnItem("name", "PROCEDURE"), aliasedReturnItem("mode", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  // Negative tests

  test("SHOW PROCEDURES YIELD (123 + xyz)") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES RETURN *") {
    failsToParse
  }

  test("SHOW EXECUTABLE PROCEDURE") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'EXECUTABLE': expected
        |  "ALIAS"
        |  "ALIASES"
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
        |  "KEY"
        |  "LOOKUP"
        |  "NODE"
        |  "POINT"
        |  "POPULATED"
        |  "PRIVILEGE"
        |  "PRIVILEGES"
        |  "PROCEDURE"
        |  "PROCEDURES"
        |  "PROPERTY"
        |  "RANGE"
        |  "REL"
        |  "RELATIONSHIP"
        |  "ROLE"
        |  "ROLES"
        |  "SERVER"
        |  "SERVERS"
        |  "SETTING"
        |  "SETTINGS"
        |  "SUPPORTED"
        |  "TEXT"
        |  "TRANSACTION"
        |  "TRANSACTIONS"
        |  "UNIQUE"
        |  "UNIQUENESS"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin
    )
  }

  test("SHOW PROCEDURE EXECUTABLE user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE EXEC") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'EXEC': expected "EXECUTABLE", "WHERE", "YIELD" or <EOF> (line 1, column 16 (offset: 15))"""
    )
  }

  test("SHOW PROCEDURE EXECUTABLE BY") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user1, user2") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER, user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE BY user, CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE CURRENT USER") {
    failsToParse
  }

  test("SHOW PROCEDURE user") {
    failsToParse
  }

  test("SHOW CURRENT USER PROCEDURE") {
    failsToParse
  }

  test("SHOW user PROCEDURE") {
    assertFailsWithMessage(
      testName,
      """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 20 (offset: 19))"""
    )
  }

  test("SHOW USER user PROCEDURE") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'PROCEDURE': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 16 (offset: 15))"""
    )
  }

  test("SHOW PROCEDURE EXECUTABLE BY USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE EXECUTABLE USER user") {
    failsToParse
  }

  test("SHOW PROCEDURE USER user") {
    failsToParse
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW PROCEDURES YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW PROCEDURES YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW PROCEDURES YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW PROCEDURES YIELD name SHOW PROCEDURES YIELD name2 RETURN name2") {
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW PROCEDURES RETURN name2 YIELD name2") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW PROCEDURE BRIEF") {
    failsToParse
  }

  test("SHOW PROCEDURE BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES BRIEF WHERE name = 'my.proc'") {
    failsToParse
  }

  test("SHOW PROCEDURE VERBOSE") {
    failsToParse
  }

  test("SHOW PROCEDURE VERBOSE OUTPUT") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW PROCEDURES VERBOSE WHERE name = 'my.proc'") {
    failsToParse
  }

  test("SHOW PROCEDURE OUTPUT") {
    failsToParse
  }

}
