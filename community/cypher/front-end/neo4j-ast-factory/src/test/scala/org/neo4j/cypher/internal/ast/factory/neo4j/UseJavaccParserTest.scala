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

import org.neo4j.cypher.internal.ast.Statement

class UseJavaccParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statement

  test("USING PERIODIC COMMIT USE db LOAD CSV FROM 'url' AS line RETURN line") {
    failsToParse
  }

  test("USE GRAPH db USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line RETURN line") {
    failsToParse
  }

  test("CALL { USE neo4j RETURN 1 AS y } RETURN y") {
    gives {
      query(
        subqueryCall(
          use(varFor("neo4j")),
          returnLit(1 -> "y")
        ),
        return_(variableReturnItem("y"))
      )
    }
  }

  test("WITH 1 AS x CALL { WITH x USE neo4j RETURN x AS y } RETURN x, y") {
    gives {
      query(
        with_(literal(1) as "x"),
        subqueryCall(
          with_(variableReturnItem("x")),
          use(varFor("neo4j")),
          return_(varFor("x") as "y")
        ),
        return_(variableReturnItem("x"), variableReturnItem("y"))
      )
    }
  }

  test("USE foo UNION ALL RETURN 1") {
    gives {
      query(
        union(
          singleQuery(use(varFor("foo"))),
          singleQuery(return_(returnItem(literal(1), "1")))
        ).all
      )
    }
  }
}
