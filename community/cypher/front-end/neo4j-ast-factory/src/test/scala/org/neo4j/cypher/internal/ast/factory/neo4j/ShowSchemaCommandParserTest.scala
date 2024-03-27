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
import org.neo4j.cypher.internal.ast.RemovedSyntax
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.ValidSyntax
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing indexes and constraints */
class ShowSchemaCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>
    // No explicit output

    test(s"SHOW $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW BTREE $indexKeyword") {
      assertAst(
        singleQuery(ShowIndexesClause(
          BtreeIndexes,
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
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
            brief = false,
            verbose = false,
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
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
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
            brief = false,
            verbose = false,
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
            brief = false,
            verbose = false,
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
          ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, List.empty, yieldAll = false)(pos)
        ),
        comparePosition = false
      )
    }

    // Brief output (deprecated)

    test(s"SHOW $indexKeyword BRIEF") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = true,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = true,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = true,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = true,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      assertAst(
        singleQuery(ShowIndexesClause(
          BtreeIndexes,
          brief = true,
          verbose = false,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    // Verbose output (deprecated)

    test(s"SHOW $indexKeyword VERBOSE") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = false,
          verbose = true,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      assertAst(
        singleQuery(ShowIndexesClause(
          AllIndexes,
          brief = false,
          verbose = true,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      assertAst(
        singleQuery(ShowIndexesClause(
          BtreeIndexes,
          brief = false,
          verbose = true,
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    assertAst(
      singleQuery(ShowIndexesClause(
        AllIndexes,
        brief = false,
        verbose = false,
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
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
          None,
          List(commandResultItem("populationPercent")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("populationPercent")))
      ),
      comparePosition = false
    )
  }

  test("SHOW BTREE INDEXES YIELD *") {
    assertAst(
      singleQuery(
        ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("SHOW RANGE INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ShowIndexesClause(RangeIndexes, brief = false, verbose = false, None, List.empty, yieldAll = true)(pos),
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
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
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
          brief = false,
          verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
        brief = false,
        verbose = false,
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
    failsToParse[Statements]
  }

  test("SHOW INDEX YIELD (123 + xyz) AS foo") {
    failsToParse[Statements]
  }

  test("SHOW ALL BTREE INDEXES") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test("SHOW INDEX OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW INDEX YIELD") {
    failsToParse[Statements]
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES BRIEF YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES VERBOSE YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES BRIEF RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES VERBOSE RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES YIELD * YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES YIELD a b RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW INDEXES RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW NODE INDEXES") {
    failsToParse[Statements]
  }

  test("SHOW REL INDEXES") {
    failsToParse[Statements]
  }

  test("SHOW RELATIONSHIP INDEXES") {
    failsToParse[Statements]
  }

  test("SHOW RANGE INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW RANGE INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW FULLTEXT INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW FULLTEXT INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW TEXT INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW TEXT INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW POINT INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW POINT INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW VECTOR INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW VECTOR INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW LOOKUP INDEXES BRIEF") {
    failsToParse[Statements]
  }

  test("SHOW LOOKUP INDEXES VERBOSE") {
    failsToParse[Statements]
  }

  test("SHOW UNKNOWN INDEXES") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'UNKNOWN': expected""")
  }

  test("SHOW BUILT IN INDEXES") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'INDEXES': expected "FUNCTION" or "FUNCTIONS"""")
  }

  // Show constraints

  private val oldConstraintTypes = Seq(
    ("", AllConstraints),
    ("ALL", AllConstraints),
    ("UNIQUE", UniqueConstraints),
    ("NODE KEY", NodeKeyConstraints),
    ("EXIST", ExistsConstraints(ValidSyntax)),
    ("EXISTS", ExistsConstraints(RemovedSyntax)),
    ("NODE EXIST", NodeExistsConstraints(ValidSyntax)),
    ("NODE EXISTS", NodeExistsConstraints(RemovedSyntax)),
    ("RELATIONSHIP EXIST", RelExistsConstraints(ValidSyntax)),
    ("RELATIONSHIP EXISTS", RelExistsConstraints(RemovedSyntax))
  )

  private val newConstraintType = Seq(
    ("NODE UNIQUE", NodeUniqueConstraints),
    ("RELATIONSHIP UNIQUE", RelUniqueConstraints),
    ("REL UNIQUE", RelUniqueConstraints),
    ("UNIQUENESS", UniqueConstraints),
    ("NODE UNIQUENESS", NodeUniqueConstraints),
    ("RELATIONSHIP UNIQUENESS", RelUniqueConstraints),
    ("REL UNIQUENESS", RelUniqueConstraints),
    ("KEY", KeyConstraints),
    ("RELATIONSHIP KEY", RelKeyConstraints),
    ("REL KEY", RelKeyConstraints),
    ("PROPERTY EXISTENCE", ExistsConstraints(ValidSyntax)),
    ("PROPERTY EXIST", ExistsConstraints(ValidSyntax)),
    ("EXISTENCE", ExistsConstraints(ValidSyntax)),
    ("NODE PROPERTY EXISTENCE", NodeExistsConstraints(ValidSyntax)),
    ("NODE PROPERTY EXIST", NodeExistsConstraints(ValidSyntax)),
    ("NODE EXISTENCE", NodeExistsConstraints(ValidSyntax)),
    ("RELATIONSHIP PROPERTY EXISTENCE", RelExistsConstraints(ValidSyntax)),
    ("RELATIONSHIP PROPERTY EXIST", RelExistsConstraints(ValidSyntax)),
    ("RELATIONSHIP EXISTENCE", RelExistsConstraints(ValidSyntax)),
    ("REL PROPERTY EXISTENCE", RelExistsConstraints(ValidSyntax)),
    ("REL PROPERTY EXIST", RelExistsConstraints(ValidSyntax)),
    ("REL EXISTENCE", RelExistsConstraints(ValidSyntax)),
    ("REL EXIST", RelExistsConstraints(ValidSyntax)),
    ("NODE PROPERTY TYPE", NodePropTypeConstraints),
    ("RELATIONSHIP PROPERTY TYPE", RelPropTypeConstraints),
    ("REL PROPERTY TYPE", RelPropTypeConstraints),
    ("PROPERTY TYPE", PropTypeConstraints)
  )

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>
      (oldConstraintTypes ++ newConstraintType).foreach {
        case (constraintTypeKeyword, constraintType) =>
          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAst(singleQuery(ShowConstraintsClause(
              constraintType,
              brief = false,
              verbose = false,
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)))
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAst(
              singleQuery(
                use(List("db")),
                ShowConstraintsClause(
                  constraintType,
                  brief = false,
                  verbose = false,
                  None,
                  List.empty,
                  yieldAll = false
                )(pos)
              ),
              comparePosition = false
            )
          }

      }

      // Brief/verbose output (removed, but throw semantic checking error)

      oldConstraintTypes.foreach {
        case (constraintTypeKeyword, constraintType) =>
          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF") {
            assertAst(singleQuery(ShowConstraintsClause(
              constraintType,
              brief = true,
              verbose = false,
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF OUTPUT") {
            assertAst(singleQuery(ShowConstraintsClause(
              constraintType,
              brief = true,
              verbose = false,
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE") {
            assertAst(singleQuery(ShowConstraintsClause(
              constraintType,
              brief = false,
              verbose = true,
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE OUTPUT") {
            assertAst(singleQuery(ShowConstraintsClause(
              constraintType,
              brief = false,
              verbose = true,
              None,
              List.empty,
              yieldAll = false
            )(defaultPos)))
          }
      }
  }

  // Show constraints filtering

  test("SHOW CONSTRAINT WHERE entityType = 'RELATIONSHIP'") {
    assertAst(
      singleQuery(ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
        Some(where(equals(varFor("entityType"), literalString("RELATIONSHIP")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW REL PROPERTY EXISTENCE CONSTRAINTS YIELD labelsOrTypes") {
    assertAst(
      singleQuery(
        ShowConstraintsClause(
          RelExistsConstraints(ValidSyntax),
          brief = false,
          verbose = false,
          None,
          List(commandResultItem("labelsOrTypes")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("labelsOrTypes")))
      ),
      comparePosition = false
    )
  }

  test("SHOW UNIQUE CONSTRAINTS YIELD *") {
    assertAst(
      singleQuery(
        ShowConstraintsClause(
          UniqueConstraints,
          brief = false,
          verbose = false,
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW CONSTRAINTS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ShowConstraintsClause(
          AllConstraints,
          brief = false,
          verbose = false,
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("USE db SHOW NODE KEY CONSTRAINTS YIELD name, properties AS pp WHERE size(pp) > 1 RETURN name") {
    assertAst(
      singleQuery(
        use(List("db")),
        ShowConstraintsClause(
          NodeKeyConstraints,
          brief = false,
          verbose = false,
          None,
          List(
            commandResultItem("name"),
            commandResultItem("properties", Some("pp"))
          ),
          yieldAll = false
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
    assertAst(
      singleQuery(
        use(List("db")),
        ShowConstraintsClause(
          AllConstraints,
          brief = false,
          verbose = false,
          None,
          List(
            commandResultItem("name"),
            commandResultItem("populationPercent", Some("pp"))
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

  test("SHOW EXISTENCE CONSTRAINTS YIELD name AS CONSTRAINT, type AS OUTPUT") {
    assertAst(
      singleQuery(
        ShowConstraintsClause(
          ExistsConstraints(ValidSyntax),
          brief = false,
          verbose = false,
          None,
          List(
            commandResultItem("name", Some("CONSTRAINT")),
            commandResultItem("type", Some("OUTPUT"))
          ),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("CONSTRAINT", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW NODE EXIST CONSTRAINTS WHERE name = 'GRANT'") {
    assertAst(
      singleQuery(ShowConstraintsClause(
        NodeExistsConstraints(ValidSyntax),
        brief = false,
        verbose = false,
        Some(where(equals(varFor("name"), literalString("GRANT")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW CONSTRAINTS YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
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

  test("SHOW CONSTRAINTS YIELD name as options, properties as name where size(name) > 0 RETURN options as name") {
    assertAst(singleQuery(
      ShowConstraintsClause(
        AllConstraints,
        brief = false,
        verbose = false,
        None,
        List(
          commandResultItem("name", Some("options")),
          commandResultItem("properties", Some("name"))
        ),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("options", "name")),
        where = Some(where(
          greaterThan(size(varFor("name")), literalInt(0))
        ))
      ),
      return_(aliasedReturnItem("options", "name"))
    ))
  }

  // Negative tests for show constraints

  test("SHOW ALL EXISTS CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW NODE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test("SHOW REL EXISTS CONSTRAINTS") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'EXISTS': expected
        |  "EXIST"
        |  "EXISTENCE"
        |  "KEY"
        |  "PROPERTY"
        |  "UNIQUE"
        |  "UNIQUENESS" (line 1, column 10 (offset: 9))""".stripMargin
    )
  }

  test("SHOW PROPERTY CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW PROP CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW PROPERTY EXISTS CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW NODE TYPE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW RELATIONSHIP TYPE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW REL TYPE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW TYPE CONSTRAINTS") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    failsToParse[Statements]
  }

  newConstraintType.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        failsToParse[Statements]
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        failsToParse[Statements]
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        failsToParse[Statements]
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        failsToParse[Statements]
      }
  }

  test("SHOW CONSTRAINT YIELD (123 + xyz)") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS YIELD (123 + xyz) AS foo") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINT YIELD") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS BRIEF YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS VERBOSE YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS BRIEF RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS VERBOSE RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS BRIEF WHERE entityType = 'NODE'") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS VERBOSE WHERE entityType = 'NODE'") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS YIELD * YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW CONSTRAINTS YIELD a b RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse[Statements]
  }

  test("SHOW NODE EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse[Statements]
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse[Statements]
  }

  test("SHOW EXISTS CONSTRAINT YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW NODE EXISTS CONSTRAINT YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT YIELD name") {
    failsToParse[Statements]
  }

  test("SHOW EXISTS CONSTRAINT RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW EXISTENCE CONSTRAINT RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW UNKNOWN CONSTRAINTS") {
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'UNKNOWN': expected""")
  }

  test("SHOW BUILT IN CONSTRAINTS") {
    assertFailsWithMessageStart[Statements](
      testName,
      """Invalid input 'CONSTRAINTS': expected "FUNCTION" or "FUNCTIONS""""
    )
  }

  // Invalid clause order tests for indexes and constraints

  for {
    prefix <- Seq("USE neo4j", "")
    entity <- Seq("INDEXES", "CONSTRAINTS")
  } {
    test(s"$prefix SHOW $entity YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW $entity YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW $entity YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW $entity RETURN name2 YIELD name2") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'RETURN': expected")
    }
  }
}
