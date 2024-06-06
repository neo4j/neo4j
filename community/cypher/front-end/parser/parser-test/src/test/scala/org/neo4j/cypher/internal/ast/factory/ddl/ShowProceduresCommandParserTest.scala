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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing procedures */
class ShowProceduresCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("PROCEDURE", "PROCEDURES").foreach { procKeyword =>
    test(s"SHOW $procKeyword") {
      assertAst(singleQuery(ShowProceduresClause(None, None, List.empty, yieldAll = false)(defaultPos)))
    }

    test(s"SHOW $procKeyword EXECUTABLE") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(CurrentUser), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT USER") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(CurrentUser), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $procKeyword EXECUTABLE BY user") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(User("user")), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $procKeyword EXECUTABLE BY CURRENT") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(User("CURRENT")), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $procKeyword EXECUTABLE BY SHOW") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(User("SHOW")), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $procKeyword EXECUTABLE BY TERMINATE") {
      assertAst(
        singleQuery(ShowProceduresClause(Some(User("TERMINATE")), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"USE db SHOW $procKeyword") {
      assertAst(SingleQuery(
        List(
          use(List("db")),
          ShowProceduresClause(None, None, List.empty, yieldAll = false)((1, 8, 7))
        )
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
          StringLiteral("my.proc")((1, 29, 28).withInputLength(9))
        )((1, 27, 26))
      )((1, 16, 15))),
      List.empty,
      yieldAll = false
    )(defaultPos)))
  }

  test("SHOW PROCEDURES YIELD description") {
    assertAst(singleQuery(
      ShowProceduresClause(
        None,
        None,
        List(commandResultItem("description")),
        yieldAll = false
      )(defaultPos),
      withFromYield(
        returnAllItems((1, 23, 22)).withDefaultOrderOnColumns(List("description"))
      )
    ))
  }

  test("SHOW PROCEDURES EXECUTABLE BY user YIELD *") {
    assertAst(singleQuery(
      ShowProceduresClause(Some(User("user")), None, List.empty, yieldAll = true)(defaultPos),
      withFromYield(returnAllItems)
    ))
  }

  test("SHOW PROCEDURES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(singleQuery(
      ShowProceduresClause(None, None, List.empty, yieldAll = true)(defaultPos),
      withFromYield(
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
        use(List.apply("db")),
        ShowProceduresClause(
          None,
          None,
          List(
            commandResultItem("name"),
            commandResultItem("description", Some("pp"))
          ),
          yieldAll = false
        )(pos),
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
    "USE db SHOW PROCEDURES EXECUTABLE YIELD name, description AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAst(
      singleQuery(
        use(List("db")),
        ShowProceduresClause(
          Some(CurrentUser),
          None,
          List(
            commandResultItem("name"),
            commandResultItem("description", Some("pp"))
          ),
          yieldAll = false
        )(pos),
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

  test("SHOW PROCEDURES YIELD name AS PROCEDURE, mode AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowProceduresClause(
          None,
          None,
          List(
            commandResultItem("name", Some("PROCEDURE")),
            commandResultItem("mode", Some("OUTPUT"))
          ),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("PROCEDURE", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW PROCEDURES YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAst(singleQuery(
      ShowProceduresClause(
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

  test("SHOW PROCEDURES YIELD name as option, option as name where size(option) > 0 RETURN option as name") {
    assertAst(singleQuery(
      ShowProceduresClause(
        None,
        None,
        List(
          commandResultItem("name", Some("option")),
          commandResultItem("option", Some("name"))
        ),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("option", "name")),
        where = Some(where(
          greaterThan(size(varFor("option")), literalInt(0))
        ))
      ),
      return_(aliasedReturnItem("option", "name"))
    ))
  }

  // Negative tests

  test("SHOW PROCEDURES YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES YIELD") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES WHERE name = 'my.proc' RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW EXECUTABLE PROCEDURE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
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
      case _ => _.withSyntaxError(
          """Invalid input 'EXECUTABLE': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
            |"SHOW EXECUTABLE PROCEDURE"
            |      ^""".stripMargin
        )
    }
  }

  test("SHOW PROCEDURE EXECUTABLE user") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXEC") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'EXEC': expected
            |  "EXECUTABLE"
            |  "SHOW"
            |  "TERMINATE"
            |  "WHERE"
            |  "YIELD"
            |  <EOF> (line 1, column 16 (offset: 15))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'EXEC': expected 'EXECUTABLE', 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF> (line 1, column 16 (offset: 15))
            |"SHOW PROCEDURE EXEC"
            |                ^""".stripMargin
        )
    }
  }

  test("SHOW PROCEDURE EXECUTABLE BY") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE BY user1, user2") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER user") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE BY CURRENT USER, user") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE BY user CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE BY user, CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE CURRENT USER") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE user") {
    failsParsing[Statements]
  }

  test("SHOW CURRENT USER PROCEDURE") {
    failsParsing[Statements]
  }

  test("SHOW user PROCEDURE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 20 (offset: 19))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 20 (offset: 19))
            |"SHOW user PROCEDURE"
            |                    ^""".stripMargin
        )
    }
  }

  test("SHOW USER user PROCEDURE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'PROCEDURE': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 16 (offset: 15))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'PROCEDURE': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 16 (offset: 15))
            |"SHOW USER user PROCEDURE"
            |                ^""".stripMargin
        )
    }
  }

  test("SHOW PROCEDURE EXECUTABLE BY USER user") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE EXECUTABLE USER user") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE USER user") {
    failsParsing[Statements]
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW PROCEDURES YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'ORDER BY'""".stripMargin
          )
      }

    }

    test(s"$prefix UNWIND range(1,10) as b SHOW PROCEDURES YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'EXECUTABLE'""".stripMargin
          )
      }

    }

    test(s"$prefix WITH 'n' as n SHOW PROCEDURES YIELD name RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'RETURN': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'EXECUTABLE'""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'EXECUTABLE'""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES WITH 1 as c") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'EXECUTABLE'""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES YIELD a WITH a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES YIELD as UNWIND as as a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'UNWIND': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>""".stripMargin
          )
      }

    }

    test(s"$prefix SHOW PROCEDURES RETURN name2 YIELD name2") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'RETURN': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'EXECUTABLE'""".stripMargin
          )
      }

    }
  }

  // Brief/verbose not allowed

  test("SHOW PROCEDURE BRIEF") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE BRIEF OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES BRIEF YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES BRIEF RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES BRIEF WHERE name = 'my.proc'") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE VERBOSE") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE VERBOSE OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES VERBOSE YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES VERBOSE RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURES VERBOSE WHERE name = 'my.proc'") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW PROCEDURE YIELD name ORDER BY name AST RETURN *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'AST': expected
            |  "!="
            |  "%"
            |  "*"
            |  "+"
            |  ","
            |  "-"
            |  "/"
            |  "::"
            |  "<"
            |  "<="
            |  "<>"
            |  "="
            |  "=~"
            |  ">"
            |  ">="
            |  "AND"
            |  "ASC"
            |  "ASCENDING"
            |  "CONTAINS"
            |  "DESC"
            |  "DESCENDING"
            |  "ENDS"
            |  "IN"
            |  "IS"
            |  "LIMIT"
            |  "OR"
            |  "RETURN"
            |  "SHOW"
            |  "SKIP"
            |  "STARTS"
            |  "TERMINATE"
            |  "WHERE"
            |  "XOR"
            |  "^"
            |  "||"
            |  <EOF> (line 1, column 41 (offset: 40))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'AST': expected an expression, ',', 'ASC', 'ASCENDING', 'DESC', 'DESCENDING', 'LIMIT', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF> (line 1, column 41 (offset: 40))
            |"SHOW PROCEDURE YIELD name ORDER BY name AST RETURN *"
            |                                         ^""".stripMargin
        )
    }
  }
}
