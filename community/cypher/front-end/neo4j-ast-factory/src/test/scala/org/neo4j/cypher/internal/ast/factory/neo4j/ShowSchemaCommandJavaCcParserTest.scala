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

import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.DeprecatedSyntax
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NewSyntax
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.OldValidSyntax
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ShowSchemaCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>

    // No explicit output

    test(s"SHOW $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW RANGE $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(RangeIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW FULLTEXT $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(FulltextIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW TEXT $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(TextIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW POINT $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(PointIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW LOOKUP $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(LookupIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"USE db SHOW $indexKeyword") {
      assertJavaCCAST(testName, query(use(varFor("db")), ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)),
        comparePosition = false)
    }

    // Brief output (deprecated)

    test(s"SHOW $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(BtreeIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    // Verbose output (deprecated)

    test(s"SHOW $indexKeyword VERBOSE") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false,
      Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))), hasYield = false)(defaultPos)), comparePosition = false)
  }

  test("SHOW INDEXES YIELD populationPercent") {
    assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("populationPercent")))), comparePosition = false)
  }

  test("SHOW POINT INDEXES YIELD populationPercent") {
    assertJavaCCAST(testName, query(ShowIndexesClause(PointIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("populationPercent")))), comparePosition = false)
  }

  test("SHOW BTREE INDEXES YIELD *") {
    assertJavaCCAST(testName, query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems)), comparePosition = false)
  }

  test("SHOW INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("SHOW RANGE INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName, query(ShowIndexesClause(RangeIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("USE db SHOW FULLTEXT INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowIndexesClause(FulltextIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("USE db SHOW BTREE INDEXES YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))),
      comparePosition = false)
  }

  test("SHOW TEXT INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowIndexesClause(TextIndexes, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))),
      comparePosition = false)
  }

  test("SHOW LOOKUP INDEXES WHERE name = 'GRANT'") {
    assertJavaCCAST(testName, query(ShowIndexesClause(LookupIndexes, brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(defaultPos)), comparePosition = false)
  }

  // Negative tests for show indexes

  test("SHOW INDEX YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW INDEX YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  test("SHOW ALL BTREE INDEXES") {
    assertJavaCCException(testName,
      """Invalid input 'BTREE': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin)
  }

  test("SHOW INDEX OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW INDEX YIELD") {
    assertSameAST(testName)
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES BRIEF YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES VERBOSE YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES BRIEF RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES VERBOSE RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES YIELD * YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES YIELD a b RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW INDEXES RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW NODE INDEXES") {
    assertSameAST(testName)
  }

  test("SHOW REL INDEXES") {
    assertSameAST(testName)
  }

  test("SHOW RELATIONSHIP INDEXES") {
    assertSameAST(testName)
  }

  test("SHOW RANGE INDEXES BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW RANGE INDEXES VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW FULLTEXT INDEXES BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW FULLTEXT INDEXES VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW TEXT INDEXES BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW TEXT INDEXES VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW POINT INDEXES BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW POINT INDEXES VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW LOOKUP INDEXES BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW LOOKUP INDEXES VERBOSE") {
    assertSameAST(testName)
  }

  // Show constraints

  private val oldConstraintTypes = Seq(
    ("", AllConstraints),
    ("ALL", AllConstraints),
    ("UNIQUE", UniqueConstraints),
    ("NODE KEY", NodeKeyConstraints),
    ("EXIST", ExistsConstraints(OldValidSyntax)),
    ("EXISTS", ExistsConstraints(DeprecatedSyntax)),
    ("NODE EXIST", NodeExistsConstraints(OldValidSyntax)),
    ("NODE EXISTS", NodeExistsConstraints(DeprecatedSyntax)),
    ("RELATIONSHIP EXIST", RelExistsConstraints(OldValidSyntax)),
    ("RELATIONSHIP EXISTS", RelExistsConstraints(DeprecatedSyntax)),
  )

  private val newExistenceConstraintType = Seq(
    ("PROPERTY EXISTENCE", ExistsConstraints(NewSyntax)),
    ("PROPERTY EXIST", ExistsConstraints(NewSyntax)),
    ("EXISTENCE", ExistsConstraints(NewSyntax)),
    ("NODE PROPERTY EXISTENCE", NodeExistsConstraints(NewSyntax)),
    ("NODE PROPERTY EXIST", NodeExistsConstraints(NewSyntax)),
    ("NODE EXISTENCE", NodeExistsConstraints(NewSyntax)),
    ("RELATIONSHIP PROPERTY EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("RELATIONSHIP PROPERTY EXIST", RelExistsConstraints(NewSyntax)),
    ("RELATIONSHIP EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL PROPERTY EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL PROPERTY EXIST", RelExistsConstraints(NewSyntax)),
    ("REL EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL EXIST", RelExistsConstraints(NewSyntax)),
  )

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>
      (oldConstraintTypes ++ newExistenceConstraintType).foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            assertJavaCCAST(testName, query(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            assertJavaCCAST(testName, query(use(varFor("db")),
              ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(defaultPos)), comparePosition = false)
          }

      }

      // Brief/verbose output (deprecated)

      oldConstraintTypes.foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF") {
            assertJavaCCAST(testName, query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF OUTPUT") {
            assertJavaCCAST(testName, query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE") {
            assertJavaCCAST(testName, query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE OUTPUT") {
            assertJavaCCAST(testName, query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
          }
      }
  }

  // Show constraints filtering

  test("SHOW CONSTRAINT WHERE entityType = 'RELATIONSHIP'") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(AllConstraints, brief = false, verbose = false,
      Some(where(equals(varFor("entityType"), literalString("RELATIONSHIP")))), hasYield = false)(defaultPos)), comparePosition = false)
  }

  test("SHOW REL PROPERTY EXISTENCE CONSTRAINTS YIELD labelsOrTypes") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(RelExistsConstraints(NewSyntax), brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("labelsOrTypes")))), comparePosition = false)
  }

  test("SHOW UNIQUE CONSTRAINTS YIELD *") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(UniqueConstraints, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems)), comparePosition = false)
  }

  test("SHOW CONSTRAINTS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("USE db SHOW NODE KEY CONSTRAINTS YIELD name, properties AS pp WHERE size(pp) > 1 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowConstraintsClause(NodeKeyConstraints, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("properties", "pp")),
        where = Some(where(greaterThan(function("size", varFor("pp")), literalInt(1))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("USE db SHOW CONSTRAINTS YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("SHOW EXISTENCE CONSTRAINTS YIELD name AS CONSTRAINT, type AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(ExistsConstraints(NewSyntax), brief = false, verbose = false, None, hasYield = true)(defaultPos),
      yieldClause(returnItems(aliasedReturnItem("name", "CONSTRAINT"), aliasedReturnItem("type", "OUTPUT")))),
      comparePosition = false)
  }

  test("SHOW NODE EXIST CONSTRAINTS WHERE name = 'GRANT'") {
    assertJavaCCAST(testName, query(ShowConstraintsClause(NodeExistsConstraints(OldValidSyntax), brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(defaultPos)),
      comparePosition = false)
  }

  // Negative tests for show constraints

  test("SHOW ALL EXISTS CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW UNIQUENESS CONSTRAINTS") {
    assertJavaCCException(testName,
      """Invalid input 'UNIQUENESS': expected
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

  test("SHOW NODE CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    assertJavaCCException(testName,
      """Invalid input 'NODES': expected
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

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    assertJavaCCException(testName,
      """Invalid input 'RELATIONSHIPS': expected
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

  test("SHOW REL EXISTS CONSTRAINTS") {
    assertJavaCCException(testName, """Invalid input 'EXISTS': expected "EXIST", "EXISTENCE" or "PROPERTY" (line 1, column 10 (offset: 9))""")
  }

  test("SHOW KEY CONSTRAINTS") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    assertSameAST(testName)
  }

  newExistenceConstraintType.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        assertSameAST(testName)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        assertSameAST(testName)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        assertSameAST(testName)
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        assertSameAST(testName)
      }
  }

  test("SHOW CONSTRAINT YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINT YIELD") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS BRIEF YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS VERBOSE YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS BRIEF RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS VERBOSE RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS BRIEF WHERE entityType = 'NODE'") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS VERBOSE WHERE entityType = 'NODE'") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS YIELD * YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW CONSTRAINTS YIELD a b RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW EXISTS CONSTRAINT WHERE name = 'foo'") {
    assertSameAST(testName)
  }

  test("SHOW NODE EXISTS CONSTRAINT WHERE name = 'foo'") {
    assertSameAST(testName)
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT WHERE name = 'foo'") {
    assertSameAST(testName)
  }

  test("SHOW EXISTS CONSTRAINT YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW NODE EXISTS CONSTRAINT YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT YIELD name") {
    assertSameAST(testName)
  }

  test("SHOW EXISTS CONSTRAINT RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW EXISTENCE CONSTRAINT RETURN *") {
    assertSameAST(testName)
  }

  // Invalid clause order tests for indexes and constraints

  for {prefix <- Seq("USE neo4j", "")
       entity <- Seq("INDEXES", "CONSTRAINTS")} {
    test(s"$prefix SHOW $entity YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW $entity YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW $entity YIELD name RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD a WITH a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD as UNWIND as as a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW $entity YIELD name SHOW $entity YIELD name2 RETURN name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity RETURN name2 YIELD name2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }
  }
}
