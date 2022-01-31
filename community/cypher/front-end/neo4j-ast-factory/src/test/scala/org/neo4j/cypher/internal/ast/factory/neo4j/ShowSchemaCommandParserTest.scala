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
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RemovedSyntax
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.ValidSyntax

/* Tests for listing indexes and constraints */
class ShowSchemaCommandParserTest extends AdministrationCommandParserTestBase {

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>

    // No explicit output

    test(s"SHOW $indexKeyword") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword") {
     assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword") {
     assertAst(query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW RANGE $indexKeyword") {
     assertAst(query(ShowIndexesClause(RangeIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW FULLTEXT $indexKeyword") {
     assertAst(query(ShowIndexesClause(FulltextIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW TEXT $indexKeyword") {
     assertAst(query(ShowIndexesClause(TextIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW POINT $indexKeyword") {
     assertAst(query(ShowIndexesClause(PointIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW LOOKUP $indexKeyword") {
     assertAst(query(ShowIndexesClause(LookupIndexes, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"USE db SHOW $indexKeyword") {
     assertAst(query(use(varFor("db")), ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = false)(pos)),
       comparePosition = false)
    }

    // Brief output (deprecated)

    test(s"SHOW $indexKeyword BRIEF") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      assertAst(query(ShowIndexesClause(BtreeIndexes, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
    }

    // Verbose output (deprecated)

    test(s"SHOW $indexKeyword VERBOSE") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      assertAst(query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false,
      Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))), hasYield = false)(pos)
    ), comparePosition = false)
  }

  test("SHOW INDEXES YIELD populationPercent") {
    assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("populationPercent")))
    ), comparePosition = false)
  }

  test("SHOW POINT INDEXES YIELD populationPercent") {
    assertAst(query(ShowIndexesClause(PointIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("populationPercent")))
    ), comparePosition = false)
  }

  test("SHOW BTREE INDEXES YIELD *") {
    assertAst(query(ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnAllItems)
    ), comparePosition = false)
  }

  test("SHOW INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("SHOW RANGE INDEXES YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(query(ShowIndexesClause(RangeIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("USE db SHOW FULLTEXT INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
    assertAst(query(
      use(varFor("db")),
      ShowIndexesClause(FulltextIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("USE db SHOW BTREE INDEXES YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertAst(query(
      use(varFor("db")),
      ShowIndexesClause(BtreeIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertAst(query(ShowIndexesClause(AllIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))
    ), comparePosition = false)
  }

  test("SHOW TEXT INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertAst(query(ShowIndexesClause(TextIndexes, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))
    ), comparePosition = false)
  }

  test("SHOW LOOKUP INDEXES WHERE name = 'GRANT'") {
    assertAst(query(ShowIndexesClause(LookupIndexes, brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)
    ), comparePosition = false)
  }

  // Negative tests for show indexes

  test("SHOW INDEX YIELD (123 + xyz)") {
    failsToParse
  }

  test("SHOW INDEX YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW ALL BTREE INDEXES") {
    assertFailsWithMessage(testName,
      """Invalid input 'BTREE': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "PRIVILEGE"
        |  "PRIVILEGES"
        |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin)
  }

  test("SHOW INDEX OUTPUT") {
    failsToParse
  }

  test("SHOW INDEX YIELD") {
    failsToParse
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES RETURN *") {
    failsToParse
  }

  test("SHOW NODE INDEXES") {
    failsToParse
  }

  test("SHOW REL INDEXES") {
    failsToParse
  }

  test("SHOW RELATIONSHIP INDEXES") {
    failsToParse
  }

  test("SHOW RANGE INDEXES BRIEF") {
    failsToParse
  }

  test("SHOW RANGE INDEXES VERBOSE") {
    failsToParse
  }

  test("SHOW FULLTEXT INDEXES BRIEF") {
    failsToParse
  }

  test("SHOW FULLTEXT INDEXES VERBOSE") {
    failsToParse
  }

  test("SHOW TEXT INDEXES BRIEF") {
    failsToParse
  }

  test("SHOW TEXT INDEXES VERBOSE") {
    failsToParse
  }

  test("SHOW POINT INDEXES BRIEF") {
    failsToParse
  }

  test("SHOW POINT INDEXES VERBOSE") {
    failsToParse
  }

  test("SHOW LOOKUP INDEXES BRIEF") {
    failsToParse
  }

  test("SHOW LOOKUP INDEXES VERBOSE") {
    failsToParse
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
    ("RELATIONSHIP EXISTS", RelExistsConstraints(RemovedSyntax)),
  )

  private val newExistenceConstraintType = Seq(
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
  )

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>
      (oldConstraintTypes ++ newExistenceConstraintType).foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAst(query(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            assertAst(query(use(varFor("db")),
              ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(pos)
            ), comparePosition = false)
          }

      }

      // Brief/verbose output (deprecated)

      oldConstraintTypes.foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF") {
            assertAst(query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF OUTPUT") {
            assertAst(query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE") {
            assertAst(query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE OUTPUT") {
            assertAst(query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(defaultPos)))
          }
      }
  }

  // Show constraints filtering

  test("SHOW CONSTRAINT WHERE entityType = 'RELATIONSHIP'") {
    assertAst(query(ShowConstraintsClause(AllConstraints, brief = false, verbose = false,
      Some(where(equals(varFor("entityType"), literalString("RELATIONSHIP")))), hasYield = false)(pos)
    ), comparePosition = false)
  }

  test("SHOW REL PROPERTY EXISTENCE CONSTRAINTS YIELD labelsOrTypes") {
    assertAst(query(ShowConstraintsClause(RelExistsConstraints(ValidSyntax), brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("labelsOrTypes")))), comparePosition = false)
  }

  test("SHOW UNIQUE CONSTRAINTS YIELD *") {
    assertAst(query(ShowConstraintsClause(UniqueConstraints, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnAllItems)), comparePosition = false)
  }

  test("SHOW CONSTRAINTS YIELD * ORDER BY name SKIP 2 LIMIT 5") {
    assertAst(query(ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name")))), Some(skip(2)), Some(limit(5)))
    ), comparePosition = false)
  }

  test("USE db SHOW NODE KEY CONSTRAINTS YIELD name, properties AS pp WHERE size(pp) > 1 RETURN name") {
    assertAst(query(
      use(varFor("db")),
      ShowConstraintsClause(NodeKeyConstraints, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("properties", "pp")),
        where = Some(where(greaterThan(function("size", varFor("pp")), literalInt(1))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("USE db SHOW CONSTRAINTS YIELD name, populationPercent AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50.0 RETURN name") {
    assertAst(query(
      use(varFor("db")),
      ShowConstraintsClause(AllConstraints, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ), comparePosition = false)
  }

  test("SHOW EXISTENCE CONSTRAINTS YIELD name AS CONSTRAINT, type AS OUTPUT") {
    assertAst(query(ShowConstraintsClause(ExistsConstraints(ValidSyntax), brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "CONSTRAINT"), aliasedReturnItem("type", "OUTPUT")))
    ), comparePosition = false)
  }

  test("SHOW NODE EXIST CONSTRAINTS WHERE name = 'GRANT'") {
    assertAst(query(ShowConstraintsClause(NodeExistsConstraints(ValidSyntax), brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)
    ), comparePosition = false)
  }

  // Negative tests for show constraints

  test("SHOW ALL EXISTS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW UNIQUENESS CONSTRAINTS") {
    assertFailsWithMessage(testName,
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
        |  "TEXT"
        |  "TRANSACTION"
        |  "TRANSACTIONS"
        |  "UNIQUE"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    assertFailsWithMessage(testName,
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
        |  "TEXT"
        |  "TRANSACTION"
        |  "TRANSACTIONS"
        |  "UNIQUE"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    assertFailsWithMessage(testName,
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
        |  "TEXT"
        |  "TRANSACTION"
        |  "TRANSACTIONS"
        |  "UNIQUE"
        |  "USER"
        |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW REL EXISTS CONSTRAINTS") {
    assertFailsWithMessage(testName, """Invalid input 'EXISTS': expected "EXIST", "EXISTENCE" or "PROPERTY" (line 1, column 10 (offset: 9))""")
  }

  test("SHOW KEY CONSTRAINTS") {
    failsToParse
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  newExistenceConstraintType.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        failsToParse
      }
  }

  test("SHOW CONSTRAINT YIELD (123 + xyz)") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW CONSTRAINT YIELD") {
    failsToParse
  }

  test("SHOW CONSTRAINTS BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS BRIEF WHERE entityType = 'NODE'") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE WHERE entityType = 'NODE'") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' RETURN *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW NODE EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINT YIELD *") {
    failsToParse
  }

  test("SHOW NODE EXISTS CONSTRAINT YIELD *") {
    failsToParse
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT YIELD name") {
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINT RETURN *") {
    failsToParse
  }

  test("SHOW EXISTENCE CONSTRAINT RETURN *") {
    failsToParse
  }

  // Invalid clause order tests for indexes and constraints

  for {prefix <- Seq("USE neo4j", "")
       entity <- Seq("INDEXES", "CONSTRAINTS")} {
    test(s"$prefix SHOW $entity YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW $entity YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW $entity YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity WITH 1 as c") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW $entity YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW $entity YIELD name SHOW $entity YIELD name2 RETURN name2") {
      assertFailsWithMessageStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW $entity RETURN name2 YIELD name2") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }
  }
}
