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

import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Expression

class CaseExpressionParserTest extends JavaccParserAstTestBase[Expression] {
  implicit private val parser: JavaccRule[Expression] = JavaccRule.CaseExpression

  test("CASE WHEN (e) THEN e ELSE null END") {
    yields {
      CaseExpression(
        None,
        List(varFor("e") -> varFor("e")),
        Some(nullLiteral)
      )
    }
  }

  test("CASE when(e) WHEN (e) THEN e ELSE null END") {
    yields {
      CaseExpression(
        Some(function("when", varFor("e"))),
        List(varFor("e") -> varFor("e")),
        Some(nullLiteral)
      )
    }
  }

  test("CASE when(v1) + 1 WHEN THEN v2 ELSE null END") {
    failsToParse
  }
}
