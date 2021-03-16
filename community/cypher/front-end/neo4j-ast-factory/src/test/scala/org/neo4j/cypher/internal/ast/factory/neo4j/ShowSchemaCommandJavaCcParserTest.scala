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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ShowSchemaCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>

    // No explicit output

    test(s"SHOW $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"USE db SHOW $indexKeyword") {
      assertJavaCCAST(testName, query(use(varFor("db")), ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    // Brief output (deprecated)

    test(s"SHOW $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = false, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    // Verbose output (deprecated)

    test(s"SHOW $indexKeyword VERBOSE") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      assertJavaCCAST(testName, query(ShowIndexesClause(all = false, brief = false, verbose = true, None, hasYield = false)(pos)))
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false, Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))), hasYield = false)(pos)))
  }

  test("SHOW INDEXES YIELD populationPercent") {
    assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("populationPercent")))))
  }

  test("SHOW BTREE INDEXES YIELD *") {
    assertJavaCCAST(testName, query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("USE db SHOW BTREE INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))))
  }

  test("SHOW INDEXES WHERE name = 'GRANT'") {
    assertJavaCCAST(testName, query(ShowIndexesClause(all = true, brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)))
  }

  // Negative tests for show indexes

  test("SHOW ALL BTREE INDEXES") {
    assertJavaCCException(testName, """Invalid input 'BTREE': expected "INDEX", "INDEXES" or "ROLES" (line 1, column 10 (offset: 9))""")
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

  test("SHOW INDEXES YIELD * WITH * MATCH (n) RETURN n") {
    // Can't parse WITH after SHOW
    assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
  }

  test("UNWIND range(1,10) as b SHOW INDEXES YIELD * RETURN *") {
    // Can't parse SHOW  after UNWIND
    assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
  }

  test("SHOW INDEXES WITH name, type RETURN *") {
    // Can't parse WITH after SHOW
    assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
  }

  test("SHOW INDEXES RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW NODE INDEXES") {
    assertJavaCCException(testName,
      """Invalid input 'NODE': expected
        |  "ALL"
        |  "BTREE"
        |  "DATABASE"
        |  "DATABASES"
        |  "DEFAULT"
        |  "HOME"
        |  "INDEX"
        |  "INDEXES"
        |  "POPULATED"
        |  "ROLES" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW REL INDEXES") {
    assertJavaCCException(testName,
      """Invalid input 'REL': expected
        |  "ALL"
        |  "BTREE"
        |  "DATABASE"
        |  "DATABASES"
        |  "DEFAULT"
        |  "HOME"
        |  "INDEX"
        |  "INDEXES"
        |  "POPULATED"
        |  "ROLES" (line 1, column 6 (offset: 5))""".stripMargin)
  }

  test("SHOW RELATIONSHIP INDEXES") {
    assertJavaCCException(testName,
      """Invalid input 'RELATIONSHIP': expected
        |  "ALL"
        |  "BTREE"
        |  "DATABASE"
        |  "DATABASES"
        |  "DEFAULT"
        |  "HOME"
        |  "INDEX"
        |  "INDEXES"
        |  "POPULATED"
        |  "ROLES" (line 1, column 6 (offset: 5))""".stripMargin)
  }

}
