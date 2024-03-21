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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing functions */
class ShowFunctionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("FUNCTION", "FUNCTIONS").foreach { funcKeyword =>
    Seq(
      ("", ast.AllFunctions),
      ("ALL", ast.AllFunctions),
      ("BUILT IN", ast.BuiltInFunctions),
      ("USER DEFINED", ast.UserDefinedFunctions)
    ).foreach { case (typeString, functionType) =>
      test(s"SHOW $typeString $funcKeyword") {
        assertAstNotAntlr(
          singleQuery(ast.ShowFunctionsClause(functionType, None, None, List.empty, yieldAll = false)(defaultPos))
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, List.empty, yieldAll = false)(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT USER") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, List.empty, yieldAll = false)(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY user") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(
              functionType,
              Some(ast.User("user")),
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(
              functionType,
              Some(ast.User("CURRENT")),
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY SHOW") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(
              functionType,
              Some(ast.User("SHOW")),
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY TERMINATE") {
        assertAstNotAntlr(
          singleQuery(
            ast.ShowFunctionsClause(
              functionType,
              Some(ast.User("TERMINATE")),
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)
          )
        )
      }

      test(s"USE db SHOW $typeString $funcKeyword") {
        assertAstNotAntlr(
          singleQuery(
            use(List("db")),
            ast.ShowFunctionsClause(functionType, None, None, List.empty, yieldAll = false)(defaultPos)
          ),
          comparePosition = false
        )
      }

    }
  }

  // Filtering tests

  test("SHOW FUNCTION WHERE name = 'my.func'") {
    assertAstNotAntlr(
      singleQuery(ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        Some(where(equals(varFor("name"), literalString("my.func")))),
        List.empty,
        yieldAll = false
      )(defaultPos)),
      comparePosition = false
    )
  }

  test("SHOW FUNCTIONS YIELD description") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("description")),
        yieldAll = false
      )(defaultPos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("description"))
      )
    ))
  }

  test("SHOW USER DEFINED FUNCTIONS EXECUTABLE BY user YIELD *") {
    assertAstNotAntlr(
      singleQuery(
        ast.ShowFunctionsClause(
          ast.UserDefinedFunctions,
          Some(ast.User("user")),
          None,
          List.empty,
          yieldAll = true
        )(defaultPos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW FUNCTIONS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAstNotAntlr(
      singleQuery(
        ast.ShowFunctionsClause(ast.AllFunctions, None, None, List.empty, yieldAll = true)(defaultPos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("USE db SHOW BUILT IN FUNCTIONS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertAstNotAntlr(
      singleQuery(
        use(List("db")),
        ast.ShowFunctionsClause(
          ast.BuiltInFunctions,
          None,
          None,
          List(
            commandResultItem("name"),
            commandResultItem("description", Some("pp"))
          ),
          yieldAll = false
        )(defaultPos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("name", "pp")),
          where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))
        ),
        return_(variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW FUNCTIONS EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAstNotAntlr(
      singleQuery(
        use(List("db")),
        ast.ShowFunctionsClause(
          ast.AllFunctions,
          Some(ast.CurrentUser),
          None,
          List(
            commandResultItem("name"),
            commandResultItem("description", Some("pp"))
          ),
          yieldAll = false
        )(defaultPos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("name", "pp")),
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

  test("SHOW ALL FUNCTIONS YIELD name AS FUNCTION, mode AS OUTPUT") {
    assertAstNotAntlr(
      singleQuery(
        ast.ShowFunctionsClause(
          ast.AllFunctions,
          None,
          None,
          List(
            commandResultItem("name", Some("FUNCTION")),
            commandResultItem("mode", Some("OUTPUT"))
          ),
          yieldAll = false
        )(defaultPos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("FUNCTION", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW FUNCTIONS YIELD a ORDER BY a WHERE a = 1") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(varFor("a")))),
        where = Some(where(equals(varFor("a"), literalInt(1))))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(notEquals(
          simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
          listOf()
        )))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
        where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(add(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))),
        where = Some(where(or(
          varFor("b"),
          AllIterablePredicate(
            varFor("x"),
            listOfInt(1, 2),
            Some(isTyped(varFor("x"), IntegerType(isNullable = true)(pos)))
          )(pos)
        )))
      )
    ))
  }

  test("SHOW FUNCTIONS YIELD name as option, category as name where size(name) > 0 RETURN option as name") {
    assertAstNotAntlr(singleQuery(
      ast.ShowFunctionsClause(
        ast.AllFunctions,
        None,
        None,
        List(
          commandResultItem("name", Some("option")),
          commandResultItem("category", Some("name"))
        ),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("option", "name")),
        where = Some(where(
          greaterThan(size(varFor("name")), literalInt(0))
        ))
      ),
      return_(aliasedReturnItem("option", "name"))
    ))
  }

  // Negative tests

  test("SHOW FUNCTIONS YIELD (123 + xyz)") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS YIELD (123 + xyz) AS foo") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS YIELD") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS YIELD * YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS YIELD a b RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW EXECUTABLE FUNCTION") {
    assertFailsWithMessage[Statements](
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
        |  "USERS"
        |  "VECTOR" (line 1, column 6 (offset: 5))""".stripMargin
    )
  }

  test("SHOW FUNCTION EXECUTABLE user") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE CURRENT USER") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXEC") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user1, user2") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER user") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER, user") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user CURRENT USER") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user, CURRENT USER") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION CURRENT USER") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION user") {
    failsToParse[Statements]
  }

  test("SHOW CURRENT USER FUNCTION") {
    failsToParse[Statements]
  }

  test("SHOW user FUNCTION") {
    failsToParse[Statements]
  }

  test("SHOW USER user FUNCTION") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY USER user") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE USER user") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION USER user") {
    failsToParse[Statements]
  }

  test("SHOW BUILT FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW BUILT-IN FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW USER FUNCTIONS") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 20 (offset: 19))"""
    )
  }

  test("SHOW USER-DEFINED FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS ALL") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS BUILT IN") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS USER DEFINED") {
    failsToParse[Statements]
  }

  test("SHOW ALL USER DEFINED FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW ALL BUILT IN FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW BUILT IN USER DEFINED FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW USER DEFINED BUILT IN FUNCTIONS") {
    failsToParse[Statements]
  }

  test("SHOW UNKNOWN FUNCTIONS") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'UNKNOWN': expected""")
  }

  test("SHOW LOOKUP FUNCTIONS") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'FUNCTIONS': expected "INDEX" or "INDEXES"""")
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW FUNCTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW FUNCTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW FUNCTIONS YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS WITH 1 as c") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW FUNCTIONS YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name2 YIELD name2") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'RETURN': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW FUNCTION BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION BRIEF OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS BRIEF YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS BRIEF RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS BRIEF WHERE name = 'my.func'") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION VERBOSE OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE WHERE name = 'my.func'") {
    failsToParse[Statements]
  }

  test("SHOW FUNCTION OUTPUT") {
    failsToParse[Statements]
  }

}
