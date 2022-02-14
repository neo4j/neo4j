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

import org.neo4j.cypher.internal.ast.Clause

class SubqueryCallParserTest extends JavaccParserAstTestBase[Clause] {

  implicit private val parser: JavaccRule[Clause] = JavaccRule.SubqueryClause

  test("CALL { RETURN 1 }") {
    gives(subqueryCall(return_(literalInt(1).unaliased)))
  }

  test("CALL { CALL { RETURN 1 as a } }") {
    gives(subqueryCall(subqueryCall(return_(literalInt(1).as("a")))))
  }

  test("CALL { RETURN 1 AS a UNION RETURN 2 AS a }") {
    gives(subqueryCall(unionDistinct(
      singleQuery(return_(literalInt(1).as("a"))),
      singleQuery(return_(literalInt(2).as("a")))
    )))
  }

  test("CALL { }") {
    failsToParse
  }

  test("CALL { CREATE (n:N) }") {
    gives(subqueryCall(create(nodePat(Some("n"), Some(labelAtom("N"))))))
  }
}
