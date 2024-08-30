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

import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing indexes and constraints */
class ShowSchemaCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>
    test(s"SHOW $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW ALL $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW RANGE $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          RangeIndexes,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW FULLTEXT $indexKeyword") {
      assertAst(
        singleQuery(
          ShowIndexesClause(
            FulltextIndexes,
            None,
            List.empty,
            yieldAll = false
          )(defaultPos)
        )
      )
    }

    test(s"SHOW TEXT $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          TextIndexes,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW POINT $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          PointIndexes,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW VECTOR $indexKeyword") {
      assertAst(
        singleQuery(
          ShowIndexesClause(
            VectorIndexes,
            None,
            List.empty,
            yieldAll = false
          )(defaultPos)
        )
      )
    }

    test(s"SHOW LOOKUP $indexKeyword") {
      assertAst(
        singleQuery(
          ShowIndexesClause(
            LookupIndexes,
            None,
            List.empty,
            yieldAll = false
          )(defaultPos)
        )
      )
    }

    test(s"USE db SHOW $indexKeyword") {
      assertAst(
        singleQuery(
          use(List("db")),
          ShowIndexesClause(AllIndexes, None, List.empty, yieldAll = false)(pos)
        ),
        comparePosition = false
      )
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    assertAst(
      singleQuery(ShowIndexesClause(
        AllIndexes,
        Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW INDEXES YIELD populationPercent") {
    assertAst(
      singleQuery(
        ShowIndexesClause(
          AllIndexes,
          None,
          List(commandResultItem("populationPercent")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("populationPercent")))
      ),
      comparePosition = false
    )
  }

  test("SHOW POINT INDEXES YIELD populationPercent") {
    assertAst(
      singleQuery(
        ShowIndexesClause(
          PointIndexes,
          None,
          List(commandResultItem("populationPercent")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("populationPercent")))
      ),
      comparePosition = false
    )
  }

  test("SHOW ALL INDEXES YIELD *") {
    assertAst(
      singleQuery(
        ShowIndexesClause(AllIndexes, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ShowIndexesClause(AllIndexes, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("SHOW RANGE INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ShowIndexesClause(RangeIndexes, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("USE db SHOW FULLTEXT INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
    assertAst(
      singleQuery(
        use(List("db")),
        ShowIndexesClause(
          FulltextIndexes,
          None,
          List(commandResultItem("name"), commandResultItem("populationPercent", Some("pp"))),
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
    "USE db SHOW VECTOR INDEXES YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAst(
      singleQuery(
        use(List("db")),
        ShowIndexesClause(
          VectorIndexes,
          None,
          List(commandResultItem("name"), commandResultItem("populationPercent", Some("pp"))),
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

  test(
    "USE db SHOW VECTOR INDEXES YIELD name, populationPercent AS pp ORDER BY pp OFFSET 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAst(
      singleQuery(
        use(List("db")),
        ShowIndexesClause(
          VectorIndexes,
          None,
          List(commandResultItem("name"), commandResultItem("populationPercent", Some("pp"))),
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

  test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowIndexesClause(
          AllIndexes,
          None,
          List(commandResultItem("name", Some("INDEX")), commandResultItem("type", Some("OUTPUT"))),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("INDEX", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TEXT INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowIndexesClause(
          TextIndexes,
          None,
          List(commandResultItem("name", Some("INDEX")), commandResultItem("type", Some("OUTPUT"))),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("INDEX", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW LOOKUP INDEXES WHERE name = 'GRANT'") {
    assertAst(
      singleQuery(ShowIndexesClause(
        LookupIndexes,
        Some(where(equals(varFor("name"), literalString("GRANT")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW INDEXES YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
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

  test("SHOW INDEXES YIELD name as options, options as name where size(options) > 0 RETURN options as name") {
    assertAst(singleQuery(
      ShowIndexesClause(
        AllIndexes,
        None,
        List(
          commandResultItem("name", Some("options")),
          commandResultItem("options", Some("name"))
        ),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("options", "name")),
        where = Some(where(
          greaterThan(size(varFor("options")), literalInt(0))
        ))
      ),
      return_(aliasedReturnItem("options", "name"))
    ))
  }

  // Negative tests for show indexes

  test("SHOW INDEX YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW INDEX YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW ALL RANGE INDEXES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'RANGE': expected
            |  "CONSTRAINT"
            |  "CONSTRAINTS"
            |  "FUNCTION"
            |  "FUNCTIONS"
            |  "INDEX"
            |  "INDEXES"
            |  "PRIVILEGE"
            |  "PRIVILEGES"
            |  "ROLE"
            |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """|Invalid input 'RANGE': expected 'CONSTRAINT', 'CONSTRAINTS', 'FUNCTION', 'FUNCTIONS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'ROLE' or 'ROLES' (line 1, column 10 (offset: 9))
             |"SHOW ALL RANGE INDEXES"
             |          ^""".stripMargin
        )
    }
  }

  test("SHOW INDEX YIELD") {
    failsParsing[Statements]
  }

  test("SHOW INDEXES YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW INDEXES YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW INDEXES RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW NODE INDEXES") {
    failsParsing[Statements]
  }

  test("SHOW REL INDEXES") {
    failsParsing[Statements]
  }

  test("SHOW RELATIONSHIP INDEXES") {
    failsParsing[Statements]
  }

  test("SHOW UNKNOWN INDEXES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'UNKNOWN': expected""")
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'UNKNOWN': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
            |"SHOW UNKNOWN INDEXES"
            |      ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'UNKNOWN': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
            |"SHOW UNKNOWN INDEXES"
            |      ^""".stripMargin
        )
    }
  }

  test("SHOW BUILT IN INDEXES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'INDEXES': expected "FUNCTION" or "FUNCTIONS"""")
      case _ => _.withSyntaxError(
          """Invalid input 'INDEXES': expected 'FUNCTION' or 'FUNCTIONS' (line 1, column 15 (offset: 14))
            |"SHOW BUILT IN INDEXES"
            |               ^""".stripMargin
        )
    }
  }

  // Syntax to be removed (collected for ease later, can then be moved to the section below)

  test("SHOW BTREE INDEX") {
    assertAst(
      singleQuery(ShowIndexesClause(
        BtreeIndexes,
        None,
        List.empty,
        yieldAll = false
      )(defaultPos))
    )
  }

  test("SHOW BTREE INDEXES") {
    assertAst(
      singleQuery(ShowIndexesClause(
        BtreeIndexes,
        None,
        List.empty,
        yieldAll = false
      )(defaultPos))
    )
  }

  test("SHOW BTREE INDEXES YIELD *") {
    assertAst(
      singleQuery(
        ShowIndexesClause(BtreeIndexes, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW BTREE INDEXES WHERE name = 'btree'") {
    assertAst(
      singleQuery(ShowIndexesClause(
        BtreeIndexes,
        Some(where(equals(varFor("name"), literalString("btree")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW ALL BTREE INDEXES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'BTREE': expected
            |  "CONSTRAINT"
            |  "CONSTRAINTS"
            |  "FUNCTION"
            |  "FUNCTIONS"
            |  "INDEX"
            |  "INDEXES"
            |  "PRIVILEGE"
            |  "PRIVILEGES"
            |  "ROLE"
            |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """|Invalid input 'BTREE': expected 'CONSTRAINT', 'CONSTRAINTS', 'FUNCTION', 'FUNCTIONS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'PRIVILEGES', 'ROLE' or 'ROLES' (line 1, column 10 (offset: 9))
             |"SHOW ALL BTREE INDEXES"
             |          ^""".stripMargin
        )
    }
  }

  // Removed syntax (also includes parts using it that was invalid anyway)

  test("SHOW INDEXES BRIEF") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW INDEX BRIEF OUTPUT") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW ALL INDEXES BRIEF") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW  ALL INDEX BRIEF OUTPUT") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW BTREE INDEXES BRIEF") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW INDEXES VERBOSE") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW ALL INDEX VERBOSE") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW BTREE INDEXES VERBOSE OUTPUT") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW INDEX OUTPUT") {
    failsParsing[Statements]
      .in {
        case Cypher5JavaCc =>
          _.withMessage(
            """Invalid input 'OUTPUT': expected
              |  "BRIEF"
              |  "SHOW"
              |  "TERMINATE"
              |  "VERBOSE"
              |  "WHERE"
              |  "YIELD"
              |  <EOF> (line 1, column 12 (offset: 11))""".stripMargin
          )
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'OUTPUT': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'OUTPUT': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW INDEXES BRIEF YIELD *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW INDEXES VERBOSE YIELD *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW INDEXES BRIEF RETURN *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW INDEXES VERBOSE RETURN *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "BRIEF")
  }

  test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW INDEXES", "VERBOSE")
  }

  test("SHOW RANGE INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW RANGE INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  test("SHOW FULLTEXT INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW FULLTEXT INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  test("SHOW TEXT INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW TEXT INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  test("SHOW POINT INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW POINT INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  test("SHOW VECTOR INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW VECTOR INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  test("SHOW LOOKUP INDEXES BRIEF") {
    assertFailsOnBriefVerboseNeverAllowed("BRIEF")
  }

  test("SHOW LOOKUP INDEXES VERBOSE") {
    assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
  }

  // Show constraints

  // initial group of constraint types, allowed brief/verbose
  private val constraintTypesV1 = Seq(
    ("", AllConstraints),
    ("ALL", AllConstraints),
    ("UNIQUE", UniqueConstraints.cypher6),
    ("NODE KEY", NodeKeyConstraints),
    ("EXIST", ExistsConstraints.cypher6),
    ("NODE EXIST", NodeExistsConstraints.cypher6),
    ("RELATIONSHIP EXIST", RelExistsConstraints.cypher6)
  )

  // group of added constraint types in 4 and 5, didn't allow brief/verbose
  private val constraintTypeV2 = Seq(
    ("NODE UNIQUE", NodeUniqueConstraints.cypher6),
    ("RELATIONSHIP UNIQUE", RelUniqueConstraints.cypher6),
    ("REL UNIQUE", RelUniqueConstraints.cypher6),
    ("UNIQUENESS", UniqueConstraints.cypher6),
    ("NODE UNIQUENESS", NodeUniqueConstraints.cypher6),
    ("RELATIONSHIP UNIQUENESS", RelUniqueConstraints.cypher6),
    ("REL UNIQUENESS", RelUniqueConstraints.cypher6),
    ("KEY", KeyConstraints),
    ("RELATIONSHIP KEY", RelKeyConstraints),
    ("REL KEY", RelKeyConstraints),
    ("PROPERTY EXISTENCE", ExistsConstraints.cypher6),
    ("PROPERTY EXIST", ExistsConstraints.cypher6),
    ("EXISTENCE", ExistsConstraints.cypher6),
    ("NODE PROPERTY EXISTENCE", NodeExistsConstraints.cypher6),
    ("NODE PROPERTY EXIST", NodeExistsConstraints.cypher6),
    ("NODE EXISTENCE", NodeExistsConstraints.cypher6),
    ("RELATIONSHIP PROPERTY EXISTENCE", RelExistsConstraints.cypher6),
    ("RELATIONSHIP PROPERTY EXIST", RelExistsConstraints.cypher6),
    ("RELATIONSHIP EXISTENCE", RelExistsConstraints.cypher6),
    ("REL PROPERTY EXISTENCE", RelExistsConstraints.cypher6),
    ("REL PROPERTY EXIST", RelExistsConstraints.cypher6),
    ("REL EXISTENCE", RelExistsConstraints.cypher6),
    ("REL EXIST", RelExistsConstraints.cypher6),
    ("NODE PROPERTY TYPE", NodePropTypeConstraints),
    ("RELATIONSHIP PROPERTY TYPE", RelPropTypeConstraints),
    ("REL PROPERTY TYPE", RelPropTypeConstraints),
    ("PROPERTY TYPE", PropTypeConstraints)
  )

  // group of constraint types added in Cypher 6 (not valid before that)
  private val constraintTypeV3 = Seq(
    ("PROPERTY UNIQUE", UniqueConstraints.cypher6),
    ("NODE PROPERTY UNIQUE", NodeUniqueConstraints.cypher6),
    ("RELATIONSHIP PROPERTY UNIQUE", RelUniqueConstraints.cypher6),
    ("REL PROPERTY UNIQUE", RelUniqueConstraints.cypher6),
    ("PROPERTY UNIQUENESS", UniqueConstraints.cypher6),
    ("NODE PROPERTY UNIQUENESS", NodeUniqueConstraints.cypher6),
    ("RELATIONSHIP PROPERTY UNIQUENESS", RelUniqueConstraints.cypher6),
    ("REL PROPERTY UNIQUENESS", RelUniqueConstraints.cypher6)
  )

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>
      (constraintTypesV1 ++ constraintTypeV2).foreach {
        case (constraintTypeKeyword, constraintType) =>
          val cypher5ConstraintType = constraintType match {
            case _: UniqueConstraints     => UniqueConstraints.cypher5
            case _: NodeUniqueConstraints => NodeUniqueConstraints.cypher5
            case _: RelUniqueConstraints  => RelUniqueConstraints.cypher5
            case _: ExistsConstraints     => ExistsConstraints.cypher5
            case _: NodeExistsConstraints => NodeExistsConstraints.cypher5
            case _: RelExistsConstraints  => RelExistsConstraints.cypher5
            case other                    => other
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAstVersionBased(
              (constraintType, returnCypher5Values) =>
                singleQuery(ShowConstraintsClause(
                  constraintType,
                  None,
                  List.empty,
                  yieldAll = false,
                  returnCypher5Values
                )(defaultPos)),
              constraintType,
              cypher5ConstraintType,
              comparePosition = true
            )
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAstVersionBased(
              (constraintType, returnCypher5Values) =>
                singleQuery(
                  use(List("db")),
                  ShowConstraintsClause(
                    constraintType,
                    None,
                    List.empty,
                    yieldAll = false,
                    returnCypher5Values
                  )(pos)
                ),
              constraintType,
              cypher5ConstraintType,
              comparePosition = false
            )
          }

      }

      constraintTypeV3.foreach {
        case (constraintTypeKeyword, constraintType) =>
          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            val errorKeyword = constraintTypeKeyword.split(" ").last
            parsesIn[Statements] {
              case Cypher5JavaCc =>
                _.withSyntaxErrorContaining(
                  s"""Invalid input '$errorKeyword': expected "EXIST", "EXISTENCE" or "TYPE" (line"""
                )
              case Cypher5 =>
                _.withSyntaxErrorContaining(
                  s"Invalid input '$errorKeyword': expected 'EXIST', 'EXISTENCE' or 'TYPE' (line"
                )
              case _ =>
                _.toAst(statementToStatements(singleQuery(ShowConstraintsClause(
                  constraintType,
                  None,
                  List.empty,
                  yieldAll = false,
                  returnCypher5Values = false
                )(defaultPos))))
            }
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            val errorKeyword = constraintTypeKeyword.split(" ").last
            parsesIn[Statements] {
              case Cypher5JavaCc =>
                _.withSyntaxErrorContaining(
                  s"""Invalid input '$errorKeyword': expected "EXIST", "EXISTENCE" or "TYPE" (line"""
                )
              case Cypher5 =>
                _.withSyntaxErrorContaining(
                  s"Invalid input '$errorKeyword': expected 'EXIST', 'EXISTENCE' or 'TYPE' (line"
                )
              case _ =>
                _.toAst(statementToStatements(singleQuery(
                  use(List("db")),
                  ShowConstraintsClause(
                    constraintType,
                    None,
                    List.empty,
                    yieldAll = false,
                    returnCypher5Values = false
                  )(pos)
                )))
            }
          }
      }
  }

  // Show constraints filtering

  test("SHOW CONSTRAINT WHERE entityType = 'RELATIONSHIP'") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(ShowConstraintsClause(
          AllConstraints,
          Some(where(equals(varFor("entityType"), literalString("RELATIONSHIP")))),
          List.empty,
          yieldAll = false,
          returnCypher5Values
        )(pos)),
      comparePosition = false
    )
  }

  test("SHOW REL PROPERTY EXISTENCE CONSTRAINTS YIELD labelsOrTypes") {
    assertAstVersionBased(
      (constraintType, returnCypher5Values) =>
        singleQuery(
          ShowConstraintsClause(
            constraintType,
            None,
            List(commandResultItem("labelsOrTypes")),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("labelsOrTypes")))
        ),
      RelExistsConstraints.cypher6,
      RelExistsConstraints.cypher5,
      comparePosition = false
    )
  }

  test("SHOW UNIQUE CONSTRAINTS YIELD *") {
    assertAstVersionBased(
      (constraintType, returnCypher5Values) =>
        singleQuery(
          ShowConstraintsClause(
            constraintType,
            None,
            List.empty,
            yieldAll = true,
            returnCypher5Values
          )(pos),
          withFromYield(returnAllItems)
        ),
      UniqueConstraints.cypher6,
      UniqueConstraints.cypher5,
      comparePosition = false
    )
  }

  test("SHOW CONSTRAINTS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List.empty,
            yieldAll = true,
            returnCypher5Values
          )(pos),
          withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
        ),
      comparePosition = false
    )
  }

  test("USE db SHOW NODE KEY CONSTRAINTS YIELD name, properties AS pp WHERE size(pp) > 1 RETURN name") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          use(List("db")),
          ShowConstraintsClause(
            NodeKeyConstraints,
            None,
            List(
              commandResultItem("name"),
              commandResultItem("properties", Some("pp"))
            ),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("name", "pp")),
            where = Some(where(greaterThan(function("size", varFor("pp")), literalInt(1))))
          ),
          return_(variableReturnItem("name"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW CONSTRAINTS YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name"
  ) {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          use(List("db")),
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(
              commandResultItem("name"),
              commandResultItem("populationPercent", Some("pp"))
            ),
            yieldAll = false,
            returnCypher5Values
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

  test("SHOW EXISTENCE CONSTRAINTS YIELD name AS CONSTRAINT, type AS OUTPUT") {
    assertAstVersionBased(
      (constraintType, returnCypher5Values) =>
        singleQuery(
          ShowConstraintsClause(
            constraintType,
            None,
            List(
              commandResultItem("name", Some("CONSTRAINT")),
              commandResultItem("type", Some("OUTPUT"))
            ),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(returnAllItems.withDefaultOrderOnColumns(List("CONSTRAINT", "OUTPUT")))
        ),
      ExistsConstraints.cypher6,
      ExistsConstraints.cypher5,
      comparePosition = false
    )
  }

  test("SHOW NODE EXIST CONSTRAINTS WHERE name = 'GRANT'") {
    assertAstVersionBased(
      (constraintType, returnCypher5Values) =>
        singleQuery(ShowConstraintsClause(
          constraintType,
          Some(where(equals(varFor("name"), literalString("GRANT")))),
          List.empty,
          yieldAll = false,
          returnCypher5Values
        )(pos)),
      NodeExistsConstraints.cypher6,
      NodeExistsConstraints.cypher5,
      comparePosition = false
    )
  }

  test("SHOW CONSTRAINTS YIELD a ORDER BY a WHERE a = 1") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a")),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(varFor("a")))),
            where = Some(where(equals(varFor("a"), literalInt(1))))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(varFor("b")))),
            where = Some(where(equals(varFor("b"), literalInt(1))))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(varFor("b")))),
            where = Some(where(equals(varFor("b"), literalInt(1))))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a")),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a")),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(notEquals(
              simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
              listOf()
            )))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
            where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
          )
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(commandResultItem("a", Some("b"))),
            yieldAll = false,
            returnCypher5Values
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
        ),
      comparePosition = true
    )
  }

  test("SHOW CONSTRAINTS YIELD name as options, properties as name where size(name) > 0 RETURN options as name") {
    assertAstVersionBased(
      returnCypher5Values =>
        singleQuery(
          ShowConstraintsClause(
            AllConstraints,
            None,
            List(
              commandResultItem("name", Some("options")),
              commandResultItem("properties", Some("name"))
            ),
            yieldAll = false,
            returnCypher5Values
          )(pos),
          withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("options", "name")),
            where = Some(where(
              greaterThan(size(varFor("name")), literalInt(0))
            ))
          ),
          return_(aliasedReturnItem("options", "name"))
        ),
      comparePosition = true
    )
  }

  // Negative tests for show constraints

  test("SHOW ALL KEY CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW NODE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'NODES': expected
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
      case Cypher5 => _.withSyntaxError(
          """|Invalid input 'NODES': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW NODES EXIST CONSTRAINTS"
             |      ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """|Invalid input 'NODES': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW NODES EXIST CONSTRAINTS"
             |      ^""".stripMargin
        )
    }
  }

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'RELATIONSHIPS': expected
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
      case Cypher5 => _.withSyntaxError(
          """|Invalid input 'RELATIONSHIPS': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW RELATIONSHIPS EXIST CONSTRAINTS"
             |      ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """|Invalid input 'RELATIONSHIPS': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW RELATIONSHIPS EXIST CONSTRAINTS"
             |      ^""".stripMargin
        )
    }
  }

  test("SHOW PROPERTY CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW PROP CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW PROPERTY EXISTS CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW NODE TYPE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW RELATIONSHIP TYPE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW REL TYPE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW TYPE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINT YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINTS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINT YIELD") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINTS YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW CONSTRAINTS YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW EXISTENCE CONSTRAINT RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW UNKNOWN CONSTRAINTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'UNKNOWN': expected")
      case Cypher5 => _.withSyntaxError(
          """|Invalid input 'UNKNOWN': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'EXISTS', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW UNKNOWN CONSTRAINTS"
             |      ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """|Invalid input 'UNKNOWN': expected 'ALIAS', 'ALIASES', 'ALL', 'BTREE', 'CONSTRAINT', 'CONSTRAINTS', 'DATABASE', 'DEFAULT DATABASE', 'HOME DATABASE', 'DATABASES', 'EXIST', 'EXISTENCE', 'FULLTEXT', 'FUNCTION', 'FUNCTIONS', 'BUILT IN', 'INDEX', 'INDEXES', 'KEY', 'LOOKUP', 'NODE', 'POINT', 'POPULATED', 'PRIVILEGE', 'PRIVILEGES', 'PROCEDURE', 'PROCEDURES', 'PROPERTY', 'RANGE', 'REL', 'RELATIONSHIP', 'ROLE', 'ROLES', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'SUPPORTED', 'TEXT', 'TRANSACTION', 'TRANSACTIONS', 'UNIQUE', 'UNIQUENESS', 'USER', 'CURRENT USER', 'USERS' or 'VECTOR' (line 1, column 6 (offset: 5))
             |"SHOW UNKNOWN CONSTRAINTS"
             |      ^""".stripMargin
        )
    }
  }

  test("SHOW BUILT IN CONSTRAINTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'CONSTRAINTS': expected "FUNCTION" or "FUNCTIONS"""")
      case _ => _.withSyntaxError(
          """Invalid input 'CONSTRAINTS': expected 'FUNCTION' or 'FUNCTIONS' (line 1, column 15 (offset: 14))
            |"SHOW BUILT IN CONSTRAINTS"
            |               ^""".stripMargin
        )
    }
  }

  // Removed syntax (also includes parts using it that was invalid anyway)

  // group of initial constraint types that were removed in 5.0 with nice error
  private val removedConstraintTypes = Seq("EXISTS", "NODE EXISTS", "RELATIONSHIP EXISTS")

  removedConstraintTypes.foreach(constraintTypeKeyword => {
    test(s"SHOW $constraintTypeKeyword CONSTRAINT") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }

    test(s"USE db SHOW $constraintTypeKeyword CONSTRAINTS") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }

    test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }

    test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF OUTPUT") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }

    test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }

    test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE OUTPUT") {
      failsParsing[Statements]
        .in {
          case Cypher5JavaCc | Cypher5 =>
            _.withSyntaxErrorContaining(
              "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."
            )
          case _ =>
            // Expected will differ depending on type
            _.withSyntaxErrorContaining("Invalid input 'EXISTS': expected ")
        }
    }
  })

  test("SHOW ALL EXISTS CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    failsParsing[Statements]
  }

  test("SHOW REL EXISTS CONSTRAINTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'EXISTS': expected
            |  "EXIST"
            |  "EXISTENCE"
            |  "KEY"
            |  "PROPERTY"
            |  "UNIQUE"
            |  "UNIQUENESS" (line 1, column 10 (offset: 9))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'EXISTS': expected 'EXIST', 'EXISTENCE', 'KEY', 'PROPERTY', 'UNIQUE' or 'UNIQUENESS' (line 1, column 10 (offset: 9))
            |"SHOW REL EXISTS CONSTRAINTS"
            |          ^""".stripMargin
        )
    }
  }

  test("SHOW EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsParsing[Statements]
  }

  test("SHOW NODE EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsParsing[Statements]
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsParsing[Statements]
  }

  test("SHOW EXISTS CONSTRAINT YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW NODE EXISTS CONSTRAINT YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT YIELD name") {
    failsParsing[Statements]
  }

  test("SHOW EXISTS CONSTRAINT RETURN *") {
    failsParsing[Statements]
  }

  constraintTypesV1.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "BRIEF")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "BRIEF")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
      }
  }

  constraintTypeV2.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        assertFailsOnBriefVerboseNeverAllowed("BRIEF")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        assertFailsOnBriefVerboseNeverAllowed("BRIEF")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        assertFailsOnBriefVerboseNeverAllowed("VERBOSE")
      }
  }

  constraintTypeV3.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        assertFailsOnBriefVerboseWhenIntroducedInCypher6("BRIEF", constraintTypeKeyword)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        assertFailsOnBriefVerboseWhenIntroducedInCypher6("BRIEF", constraintTypeKeyword)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        assertFailsOnBriefVerboseWhenIntroducedInCypher6("VERBOSE", constraintTypeKeyword)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        assertFailsOnBriefVerboseWhenIntroducedInCypher6("VERBOSE", constraintTypeKeyword)
      }
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    failsParsing[Statements]
      .in {
        case Cypher5JavaCc =>
          _.withMessage(
            """Invalid input 'OUTPUT': expected
              |  "BRIEF"
              |  "SHOW"
              |  "TERMINATE"
              |  "VERBOSE"
              |  "WHERE"
              |  "YIELD"
              |  <EOF> (line 1, column 18 (offset: 17))""".stripMargin
          )
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            "Invalid input 'OUTPUT': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"
          )
        case _ =>
          _.withSyntaxErrorContaining("Invalid input 'OUTPUT': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>")
      }
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
  }

  test("SHOW CONSTRAINTS BRIEF YIELD *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "BRIEF")
  }

  test("SHOW CONSTRAINTS VERBOSE YIELD *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
  }

  test("SHOW CONSTRAINTS BRIEF RETURN *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "BRIEF")
  }

  test("SHOW CONSTRAINTS VERBOSE RETURN *") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
  }

  test("SHOW CONSTRAINTS BRIEF WHERE entityType = 'NODE'") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "BRIEF")
  }

  test("SHOW CONSTRAINTS VERBOSE WHERE entityType = 'NODE'") {
    assertFailsOnBriefVerbosePreviouslyAllowed("SHOW CONSTRAINTS", "VERBOSE")
  }

  // Invalid clause order tests for indexes and constraints

  for {
    prefix <- Seq("USE neo4j", "")
    entity <- Seq("INDEXES", "CONSTRAINTS")
  } {
    test(s"$prefix SHOW $entity YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'ORDER BY'""".stripMargin
          )
      }
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW $entity YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>""".stripMargin
          )
      }
    }

    test(s"$prefix SHOW $entity WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix WITH 'n' as n SHOW $entity YIELD name RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'SHOW': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'SHOW': expected 'FOREACH', ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'RETURN': expected")
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity WITH 1 as c RETURN name as numIndexes") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity WITH 1 as c") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity YIELD a WITH a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'WITH': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity YIELD as UNWIND as as a RETURN a") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'UNWIND': expected")
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'UNWIND': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>"""
          )
      }
    }

    test(s"$prefix SHOW $entity RETURN name2 YIELD name2") {
      failsParsing[Statements].in {
        case Cypher5JavaCc => _.withMessageStart("Invalid input 'RETURN': expected")
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'BRIEF', 'SHOW', 'TERMINATE', 'VERBOSE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
      }
    }
  }

  // Help methods

  private def assertAstVersionBased(
    expected: (ShowConstraintType, Boolean) => Statements,
    constraintType: ShowConstraintType,
    constraintTypeCypher5: ShowConstraintType,
    comparePosition: Boolean
  ) =
    if (comparePosition)
      parsesIn[Statements] {
        case Cypher5 | Cypher5JavaCc => _.toAstPositioned(expected(constraintTypeCypher5, true))
        case _                       => _.toAstPositioned(expected(constraintType, false))
      }
    else
      parsesIn[Statements] {
        case Cypher5 | Cypher5JavaCc => _.toAst(expected(constraintTypeCypher5, true))
        case _                       => _.toAst(expected(constraintType, false))
      }

  private def assertAstVersionBased(expected: Boolean => Statements, comparePosition: Boolean) =
    if (comparePosition)
      parsesIn[Statements] {
        case Cypher5 | Cypher5JavaCc => _.toAstPositioned(expected(true))
        case _                       => _.toAstPositioned(expected(false))
      }
    else
      parsesIn[Statements] {
        case Cypher5 | Cypher5JavaCc => _.toAst(expected(true))
        case _                       => _.toAst(expected(false))
      }

  private def assertFailsOnBriefVerbosePreviouslyAllowed(command: String, keyword: String) = {
    failsParsing[Statements]
      .in {
        case Cypher5JavaCc | Cypher5 =>
          _.withSyntaxErrorContaining(
            s"""`$command` no longer allows the `BRIEF` and `VERBOSE` keywords,
               |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            s"Invalid input '$keyword': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  private def assertFailsOnBriefVerboseNeverAllowed(keyword: String) = {
    failsParsing[Statements]
      .in {
        case Cypher5JavaCc =>
          _.withMessageStart(
            s"""Invalid input '$keyword': expected
               |  "SHOW"
               |  "TERMINATE"
               |  "WHERE"
               |  "YIELD"
               |  <EOF> (line""".stripMargin
          )
        case _ =>
          _.withSyntaxErrorContaining(
            s"Invalid input '$keyword': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }

  private def assertFailsOnBriefVerboseWhenIntroducedInCypher6(keyword: String, constraintTypeKeyword: String) = {
    val errorKeyword = constraintTypeKeyword.split(" ").last
    failsParsing[Statements]
      .in {
        case Cypher5JavaCc =>
          _.withSyntaxErrorContaining(
            s"""Invalid input '$errorKeyword': expected "EXIST", "EXISTENCE" or "TYPE" (line"""
          )
        case Cypher5 =>
          _.withSyntaxErrorContaining(
            s"Invalid input '$errorKeyword': expected 'EXIST', 'EXISTENCE' or 'TYPE' (line"
          )
        case _ =>
          _.withSyntaxErrorContaining(
            s"Invalid input '$keyword': expected 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"
          )
      }
  }
}
