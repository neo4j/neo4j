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

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.util.ASTNode

class MiscParserTest extends ParserSyntaxTreeBase[ParserRuleContext, ASTNode] {

  test("RETURN 1 AS x //l33t comment") {
    implicit val javaccRule: JavaccRule[Statement] = JavaccRule.Statement
    implicit val antlrRule: AntlrRule[Cst.Statement] = AntlrRule.Statement

    gives {
      singleQuery(returnLit(1 -> "x"))
    }
  }

  test("keywords are allowed names") {
    implicit val javaccRule: JavaccRule[Statement] = JavaccRule.Statement
    implicit val antlrRule: AntlrRule[Cst.Statement] = AntlrRule.Statement

    val keywords =
      Seq(
        "TRUE",
        "FALSE",
        "NULL",
        "RETURN",
        "CREATE",
        "DELETE",
        "SET",
        "REMOVE",
        "DETACH",
        "MATCH",
        "WITH",
        "UNWIND",
        "USE",
        "GRAPH",
        "CALL",
        "YIELD",
        "LOAD",
        "CSV",
        "PERIODIC",
        "COMMIT",
        "HEADERS",
        "FROM",
        "FIELDTERMINATOR",
        "FOREACH",
        "WHERE",
        "DISTINCT",
        "MERGE",
        "OPTIONAL",
        "USING",
        "ORDER",
        "BY",
        "ASC",
        "ASCENDING",
        "DESC",
        "DESCENDING",
        "SKIP",
        "LIMIT",
        "UNION",
        "DROP",
        "INDEX",
        "SEEK",
        "SCAN",
        "JOIN",
        "CONSTRAINT",
        "ASSERT",
        "IS",
        "NODE",
        "KEY",
        "UNIQUE",
        "ON",
        "AS",
        "OR",
        "XOR",
        "AND",
        "NOT",
        "NFC",
        "NFD",
        "NFKC",
        "NFKD",
        "STARTS",
        "ENDS",
        "CONTAINS",
        "IN",
        "count",
        "FILTER",
        "EXTRACT",
        "REDUCE",
        "ROW",
        "ROWS",
        "EXISTS",
        "ALL",
        "ANY",
        "NONE",
        "SINGLE",
        "CASE",
        "ELSE",
        "WHEN",
        "THEN",
        "END",
        "shortestPath",
        "allShortestPaths"
      )

    for (keyword <- keywords) {
      parsing(s"WITH $$$keyword AS x RETURN x AS $keyword")
    }
  }

  test("should allow chained map access in SET/REMOVE") {
    implicit val javaccRule: JavaccRule[Clause] = JavaccRule.Clause
    implicit val antlrRule: AntlrRule[Cst.Clause] = AntlrRule.Clause

    val chainedProperties = prop(prop(varFor("map"), "node"), "property")

    parsing("SET map.node.property = 123") shouldGive
      SetClause(Seq(
        SetPropertyItem(chainedProperties, literal(123))(pos)
      )) _

    parsing("REMOVE map.node.property") shouldGive
      Remove(Seq(
        RemovePropertyItem(chainedProperties)
      )) _
  }

  test("should allow True and False as label name") {
    implicit val javaccRule: JavaccRule[NodePattern] = JavaccRule.NodePattern
    implicit val antlrRule: AntlrRule[Cst.NodePattern] = AntlrRule.NodePattern

    parsing("(:True)") shouldGive NodePattern(None, Some(labelLeaf("True")), None, None) _
    parsing("(:False)") shouldGive NodePattern(None, Some(labelLeaf("False")), None, None) _

    parsing("(t:True)") shouldGive nodePat(name = Some("t"), labelExpression = Some(labelLeaf("True")))
    parsing("(f:False)") shouldGive nodePat(name = Some("f"), labelExpression = Some(labelLeaf("False")))
  }

  test("-[:Person*1..2]-") {
    implicit val javaccRule: JavaccRule[RelationshipPattern] = JavaccRule.RelationshipPattern
    implicit val antlrRule: AntlrRule[Cst.RelationshipPattern] = AntlrRule.RelationshipPattern

    yields {
      RelationshipPattern(
        None,
        Some(labelRelTypeLeaf("Person")),
        Some(Some(
          Range(
            Some(literalUnsignedInt(1)),
            Some(literalUnsignedInt(2))
          )(pos)
        )),
        None,
        None,
        SemanticDirection.BOTH
      )
    }
  }

  test("should not parse list literal as pattern comprehension") {
    implicit val javaccRule: JavaccRule[Expression] = JavaccRule.Expression
    implicit val antlrRule: AntlrRule[Cst.Expression] = AntlrRule.Expression

    val listLiterals = Seq(
      "[x = '1']",
      "[x = ()--()]",
      "[x = ()--()--()--()--()--()--()--()--()--()--()]"
    )
    for (l <- listLiterals) withClue(l) {
      parsing(l) shouldVerify (_ shouldBe a[ListLiteral])
    }
  }

  test("should not parse pattern comprehensions with single nodes") {
    implicit val javaccRule: JavaccRule[Expression] = JavaccRule.PatternComprehension
    implicit val antlrRule: AntlrRule[Cst.PatternComprehension] = AntlrRule.PatternComprehension

    assertFails("[p = (x) | p]")
  }

  test("should handle escaping in string literals") {
    implicit val javaccRule: JavaccRule[Expression] = JavaccRule.StringLiteral
    implicit val antlrRule: AntlrRule[CypherParser.StringLiteralContext] = AntlrRule.StringLiteral

    parsing("""'\\\''""") shouldGive literalString("""\'""")
  }

  test("Normal Form is only converted to strings inside functions, else treated as a variable") {
    implicit val javaccRule: JavaccRule[Clause] = JavaccRule.Clause
    implicit val antlrRule: AntlrRule[Cst.Clause] = AntlrRule.Clause

    Seq("NFC", "NFD", "NFKC", "NFKD").foreach { normalForm =>
      parsing(s"RETURN $normalForm") shouldGive
        return_(variableReturnItem(normalForm))
    }
  }
}
