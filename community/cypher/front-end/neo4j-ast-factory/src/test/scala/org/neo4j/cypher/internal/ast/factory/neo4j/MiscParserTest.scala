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

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.InputPosition

class MiscParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("RETURN 1 AS x //l33t comment") {
    gives[Statement] {
      singleQuery(returnLit(1 -> "x"))
    }
  }

  test("keywords are allowed names") {
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
      parsing[Statement](s"WITH $$$keyword AS x RETURN x AS $keyword")
    }
  }

  test("should allow chained map access in SET/REMOVE") {
    val chainedProperties = prop(prop(varFor("map"), "node"), "property")

    parsing[Clause]("SET map.node.property = 123") shouldGive
      SetClause(Seq(
        SetPropertyItem(chainedProperties, literal(123))(pos)
      )) _

    parsing[Clause]("REMOVE map.node.property") shouldGive
      Remove(Seq(
        RemovePropertyItem(chainedProperties)
      )) _
  }

  test("should allow True and False as label name") {
    parsing[NodePattern]("(:True)") shouldGive NodePattern(None, Some(labelLeaf("True")), None, None) _
    parsing[NodePattern]("(:False)") shouldGive NodePattern(None, Some(labelLeaf("False")), None, None) _

    parsing[NodePattern]("(t:True)") shouldGive nodePat(name = Some("t"), labelExpression = Some(labelLeaf("True")))
    parsing[NodePattern]("(f:False)") shouldGive nodePat(name = Some("f"), labelExpression = Some(labelLeaf("False")))
  }

  test("-[:Person*1..2]-") {
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
    val listLiterals = Seq(
      "[x = '1']",
      "[x = ()--()]",
      "[x = ()--()--()--()--()--()--()--()--()--()--()]"
    )
    for (l <- listLiterals) withClue(l) {
      parsing[Expression](l) shouldVerify (_ shouldBe a[ListLiteral])
    }
  }

  test("should not parse pattern comprehensions with single nodes") {
    assertFails[PatternComprehension]("[p = (x) | p]")
  }

  test("should handle escaping in string literals") {
    """'\\\''""" should parseTo[StringLiteral](literalString("""\'"""))
  }

  test("Normal Form is only converted to strings inside functions, else treated as a variable") {
    Seq("NFC", "NFD", "NFKC", "NFKD").foreach { normalForm =>
      parsing[Clause](s"RETURN $normalForm") shouldGive
        return_(variableReturnItem(normalForm))
    }
  }

  // TODO Enable in ANTLR when possible
  test("Unicode escape outside of string literals") {
    // https://neo4j.com/docs/cypher-manual/current/syntax/parsing/#_using_unicodes_in_cypher
    "M\\u0041TCH (m) RETURN m" should parseAs[Statement]
      .parseIn(JavaCc)(_.toAstPositioned(
        SingleQuery(Seq(
          Match(
            optional = false,
            DifferentRelationships(implicitlyCreated = true)(InputPosition(0, 1, 1)),
            ForMatch(List(PatternPartWithSelector(
              AllPaths()(InputPosition(11, 1, 12)),
              PathPatternPart(NodePattern(Some(varFor("m")), None, None, None)(InputPosition(11, 1, 12)))
            )))(InputPosition(11, 1, 12)),
            List(),
            None
          )(InputPosition(0, 1, 1)),
          Return(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              List(UnaliasedReturnItem(varFor("m"), "m")(InputPosition(22, 1, 23))),
              None
            )(InputPosition(22, 1, 23)),
            None,
            None,
            None,
            Set()
          )(InputPosition(15, 1, 16))
        ))(InputPosition(15, 1, 16))
      ))
  }
}
