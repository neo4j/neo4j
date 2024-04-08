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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.exceptions.SyntaxException

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
        assertAst(
          singleQuery(ast.ShowFunctionsClause(functionType, None, None, List.empty, yieldAll = false)(defaultPos))
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE") {
        assertAst(
          singleQuery(
            ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, List.empty, yieldAll = false)(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY CURRENT USER") {
        assertAst(
          singleQuery(
            ast.ShowFunctionsClause(functionType, Some(ast.CurrentUser), None, List.empty, yieldAll = false)(defaultPos)
          )
        )
      }

      test(s"SHOW $typeString $funcKeyword EXECUTABLE BY user") {
        assertAst(
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
        assertAst(
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
        assertAst(
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
        assertAst(
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
        assertAst(
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
    assertAst(
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
    assertAst(singleQuery(
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
    assertAst(
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
    assertAst(
      singleQuery(
        ast.ShowFunctionsClause(ast.AllFunctions, None, None, List.empty, yieldAll = true)(defaultPos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("USE db SHOW BUILT IN FUNCTIONS YIELD name, description AS pp WHERE pp < 50.0 RETURN name") {
    assertAst(
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
    assertAst(
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
    assertAst(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    assertAst(singleQuery(
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
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS YIELD") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS WHERE name = 'my.func' RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW EXECUTABLE FUNCTION") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessage(
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
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Extraneous input 'EXECUTABLE': expected 'ALIAS', 'ALIASES', 'NODE', 'REL', 'RELATIONSHIP', 'UNIQUENESS', 'EXISTENCE', 'PROPERTY', 'KEY', 'EXISTS', 'ALL', 'UNIQUE', 'EXIST', 'CONSTRAINT', 'CONSTRAINTS', 'CURRENT', 'DATABASE', 'DATABASES', 'DEFAULT', 'HOME', 'BUILT', 'USER', 'FUNCTIONS', 'FULLTEXT', 'LOOKUP', 'POINT', 'RANGE', 'TEXT', 'VECTOR', 'BTREE', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'ROLE', 'ROLES', 'POPULATED', 'SERVER', 'SERVERS', 'SETTING', 'SUPPORTED', 'TRANSACTION', 'TRANSACTIONS', 'USERS' (line 1, column 6 (offset: 5))
          |"SHOW EXECUTABLE FUNCTION"
          |      ^""".stripMargin
      ))
  }

  test("SHOW FUNCTION EXECUTABLE user") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXEC") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user1, user2") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER user") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY CURRENT USER, user") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY user, CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION user") {
    failsParsing[Statements]
  }

  test("SHOW CURRENT USER FUNCTION") {
    failsParsing[Statements]
  }

  test("SHOW user FUNCTION") {
    failsParsing[Statements]
  }

  test("SHOW USER user FUNCTION") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE BY USER user") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION EXECUTABLE USER user") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION USER user") {
    failsParsing[Statements]
  }

  test("SHOW BUILT FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW BUILT-IN FUNCTIONS") {
    failsParsing[Statements]
  }

  // TODO check message missing comma
  test("SHOW USER FUNCTIONS") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessage(
        """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 20 (offset: 19))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Missing 'PRIVILEGE', 'PRIVILEGES' at '' (line 1, column 20 (offset: 19))
          |"SHOW USER FUNCTIONS"
          |                    ^""".stripMargin
      ))
  }

  test("SHOW USER-DEFINED FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS ALL") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS BUILT IN") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS USER DEFINED") {
    failsParsing[Statements]
  }

  test("SHOW ALL USER DEFINED FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW ALL BUILT IN FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW BUILT IN USER DEFINED FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW USER DEFINED BUILT IN FUNCTIONS") {
    failsParsing[Statements]
  }

  test("SHOW UNKNOWN FUNCTIONS") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'UNKNOWN': expected"""))
      .parseIn(Antlr)(_.withMessage(
        """Extraneous input 'UNKNOWN': expected 'ALIAS', 'ALIASES', 'NODE', 'REL', 'RELATIONSHIP', 'UNIQUENESS', 'EXISTENCE', 'PROPERTY', 'KEY', 'EXISTS', 'ALL', 'UNIQUE', 'EXIST', 'CONSTRAINT', 'CONSTRAINTS', 'CURRENT', 'DATABASE', 'DATABASES', 'DEFAULT', 'HOME', 'BUILT', 'USER', 'FUNCTIONS', 'FULLTEXT', 'LOOKUP', 'POINT', 'RANGE', 'TEXT', 'VECTOR', 'BTREE', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'ROLE', 'ROLES', 'POPULATED', 'SERVER', 'SERVERS', 'SETTING', 'SUPPORTED', 'TRANSACTION', 'TRANSACTIONS', 'USERS' (line 1, column 6 (offset: 5))
          |"SHOW UNKNOWN FUNCTIONS"
          |      ^""".stripMargin
      ))
  }

  test("SHOW LOOKUP FUNCTIONS") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'FUNCTIONS': expected "INDEX" or "INDEXES""""))
      .parseIn(Antlr)(_.withMessage(
        """Mismatched input 'FUNCTIONS': expected 'INDEX', 'INDEXES' (line 1, column 13 (offset: 12))
          |"SHOW LOOKUP FUNCTIONS"
          |             ^""".stripMargin
      ))
  }

  // Invalid clause order

  // TODO Unhelpful message
  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW FUNCTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WITH': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'WITH': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix UNWIND range(1,10) as b SHOW FUNCTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'SHOW': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'SHOW': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WITH': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'WITH': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix WITH 'n' as n SHOW FUNCTIONS YIELD name RETURN name as numIndexes") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'SHOW': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'SHOW': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS RETURN name as numIndexes") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'RETURN': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'RETURN': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS WITH 1 as c RETURN name as numIndexes") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WITH': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'WITH': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS WITH 1 as c") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WITH': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'WITH': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS YIELD a WITH a RETURN a") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WITH': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'WITH': expected ';', <EOF>""".stripMargin
        ))
    }

    // TODO Unhelpful message
    test(s"$prefix SHOW FUNCTIONS YIELD as UNWIND as as a RETURN a") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'UNWIND': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'UNWIND': expected ';', <EOF>""".stripMargin
        ))
    }

    test(s"$prefix SHOW FUNCTIONS RETURN name2 YIELD name2") {
      testName should notParse[Statements]
        .parseIn(JavaCc)(_.withMessageStart("Invalid input 'RETURN': expected"))
        .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
          """Mismatched input 'RETURN': expected ';', <EOF>""".stripMargin
        ))
    }
  }

  // Brief/verbose not allowed

  test("SHOW FUNCTION BRIEF") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION BRIEF OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS BRIEF YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS BRIEF RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS BRIEF WHERE name = 'my.func'") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION VERBOSE") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION VERBOSE OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTIONS VERBOSE WHERE name = 'my.func'") {
    failsParsing[Statements]
  }

  test("SHOW FUNCTION OUTPUT") {
    failsParsing[Statements]
  }

}
