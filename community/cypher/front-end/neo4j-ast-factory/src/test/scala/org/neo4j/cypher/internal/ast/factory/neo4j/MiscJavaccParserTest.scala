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
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection

class MiscJavaccParserTest extends JavaccParserAstTestBase[Any] {

  test("RETURN 1 AS x //l33t comment") {
    implicit val parser: JavaccRule[Statement] = JavaccRule.Statement
    gives {
      query(returnLit(1 -> "x"))
    }
  }

  test("keywords are allowed names") {
    implicit val parser: JavaccRule[Statement] = JavaccRule.Statement
    val keywords =
      Seq("TRUE", "FALSE", "NULL", "RETURN", "CREATE", "DELETE", "SET", "REMOVE", "DETACH", "MATCH", "WITH",
        "UNWIND", "USE", "GRAPH", "CALL", "YIELD", "LOAD", "CSV", "PERIODIC", "COMMIT",
        "HEADERS", "FROM", "FIELDTERMINATOR", "FOREACH", "WHERE", "DISTINCT", "MERGE",
        "OPTIONAL", "USING", "ORDER", "BY", "ASC", "ASCENDING", "DESC", "DESCENDING",
        "SKIP", "LIMIT", "UNION", "DROP", "INDEX", "SEEK", "SCAN", "JOIN", "CONSTRAINT",
        "ASSERT", "IS", "NODE", "KEY", "UNIQUE", "ON", "AS", "OR", "XOR", "AND", "NOT",
        "STARTS", "ENDS", "CONTAINS", "IN", "count", "FILTER", "EXTRACT", "REDUCE", "ROW", "ROWS",
        "EXISTS", "ALL", "ANY", "NONE", "SINGLE", "CASE", "ELSE", "WHEN", "THEN", "END",
        "shortestPath", "allShortestPaths")

    for (keyword <- keywords) {
      parsing(s"WITH $$$keyword AS x RETURN x AS $keyword")
    }
  }

  test("should allow chained map access in SET/REMOVE") {
    implicit val parser: JavaccRule[Clause] = JavaccRule.Clause

    val chainedProperties = prop(prop(varFor("map"), "node"),"property")

    parsing("SET map.node.property = 123") shouldGive
      SetClause(Seq(
        SetPropertyItem(chainedProperties, literal(123))(pos)
      ))_

    parsing("REMOVE map.node.property") shouldGive
      Remove(Seq(
        RemovePropertyItem(chainedProperties)
      ))_
  }

  test("should allow True and False as label name") {
    implicit val parser: JavaccRule[NodePattern] = JavaccRule.NodePattern

    parsing("(:True)") shouldGive NodePattern(None, Seq(labelName("True")), None, None, None)_
    parsing("(:False)") shouldGive NodePattern(None, Seq(labelName("False")), None, None, None)_

    parsing("(t:True)") shouldGive nodePat("t", "True")
    parsing("(f:False)") shouldGive nodePat("f", "False")
  }

  test("-[:Person*1..2]-") {
    implicit val parser: JavaccRule[RelationshipPattern] = JavaccRule.RelationshipPattern
    yields {
      RelationshipPattern(None, List(relTypeName("Person")),
        Some(Some(
          Range(
            Some(literalUnsignedInt(1)),
            Some(literalUnsignedInt(2)))(pos)
        )),
        None,
        None,
        SemanticDirection.BOTH
      )
    }
  }

  test("should not parse list literal as pattern comprehension") {
    implicit val parser: JavaccRule[Expression] = JavaccRule.Expression

    val listLiterals = Seq(
      "[x = '1']",
      "[x = ()--()]",
      "[x = ()--()--()--()--()--()--()--()--()--()--()]",
    )
    for (l <- listLiterals) withClue(l) {
      parsing(l) shouldVerify (_ shouldBe a[ListLiteral])
    }
  }

  test("should not parse pattern comprehensions with single nodes") {
    implicit val parser: JavaccRule[Expression] = JavaccRule.PatternComprehension
    assertFails("[p = (x) | p]")
  }

  test("should handle escaping in string literals") {
    implicit val parser: JavaccRule[Expression] = JavaccRule.StringLiteral
    parsing("""'\\\''""") shouldGive literalString("""\'""")
  }
}
