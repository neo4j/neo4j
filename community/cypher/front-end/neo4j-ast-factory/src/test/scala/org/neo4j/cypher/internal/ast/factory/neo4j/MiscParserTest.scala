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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

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

  test("map expression") {
    "{a:1}" should parseTo[MapExpression](mapOf("a" -> literal(1)))
    "{a:1,b:2,c:3}" should parseTo[MapExpression](mapOf("a" -> literal(1), "b" -> literal(2), "c" -> literal(3)))
    "{}" should parseTo[Expression](mapOf())
    // Not sure if should be allowed, but behaves as before at least
    "{,a:1}" should parseTo[MapExpression](mapOf("a" -> literal(1)))
    "{ name: 'Andres' }" should parseTo[Expression](mapOf("name" -> literal("Andres")))
    "{ meta : { name: 'Andres' } }" should parseTo[Expression](mapOf("meta" -> mapOf("name" -> literal("Andres"))))
  }

  test("all(item IN list WHERE predicate)") {
    parsesTo[Expression](allInList(varFor("item"), varFor("list"), varFor("predicate")))
  }

  test("all(item IN list)") {
    parsesTo[Expression](AllIterablePredicate(varFor("item"), varFor("list"), None)(pos))
  }

  test("any(item IN list WHERE predicate)") {
    parsesTo[Expression](anyInList(varFor("item"), varFor("list"), varFor("predicate")))
  }

  test("any(item IN list)") {
    parsesTo[Expression](AnyIterablePredicate(varFor("item"), varFor("list"), None)(pos))
  }

  test("none(item IN list WHERE predicate)") {
    parsesTo[Expression](noneInList(varFor("item"), varFor("list"), varFor("predicate")))
  }

  test("none(item IN list)") {
    parsesTo[Expression](NoneIterablePredicate(varFor("item"), varFor("list"), None)(pos))
  }

  test("single(item IN list WHERE predicate)") {
    parsesTo[Expression](singleInList(varFor("item"), varFor("list"), varFor("predicate")))
  }

  test("single(item IN list)") {
    parsesTo[Expression](SingleIterablePredicate(varFor("item"), varFor("list"), None)(pos))
  }

  test("$123") {
    parsesTo[Expression](parameter("123", CTAny))
  }

  test("$a") {
    parsesTo[Expression](parameter("a", CTAny))
  }

  test("[1,2,3,4][1..2]") {
    parses[Expression].toAsts {
      case JavaCc => sliceFull(listOf(literal(1), literal(2), literal(3), literal(4)), literal(1), literal(2))
      case Antlr  => sliceFull(null, literal(1), literal(2))
    }
  }

  test("[1,2,3,4][1..2][2..3]") {
    parses[Expression].toAsts {
      case JavaCc => sliceFull(
          sliceFull(listOf(literal(1), literal(2), literal(3), literal(4)), literal(1), literal(2)),
          literal(2),
          literal(3)
        )
      case Antlr => sliceFull(
          sliceFull(null, literal(1), literal(2)),
          literal(2),
          literal(3)
        )
    }
  }

  test("collection[1..2]") {
    parsesTo[Expression](sliceFull(varFor("collection"), literal(1), literal(2)))
  }

  test("[1,2,3,4][2]") {
    parses[Expression].toAsts {
      case JavaCc => containerIndex(listOf(literal(1), literal(2), literal(3), literal(4)), 2)
      case Antlr  => containerIndex(null, 2)
    }
  }

  test("[[1,2]][0][6]") {
    parses[Expression].toAsts {
      case JavaCc => containerIndex(containerIndex(listOf(listOf(literal(1), literal(2))), 0), 6)
      case Antlr  => containerIndex(containerIndex(null, 0), 6)
    }
  }

  test("collection[1..2][0]") {
    parsesTo[Expression](containerIndex(sliceFull(varFor("collection"), literal(1), literal(2)), 0))
  }

  test("collection[..-2]") {
    parsesTo[Expression](sliceTo(varFor("collection"), literal(-2)))
  }

  test("collection[1..]") {
    parsesTo[Expression](sliceFrom(varFor("collection"), literal(1)))
  }

  test("{ name: 'Andres' }") {
    parsesTo[Expression](mapOf(("name", literal("Andres"))))
  }

  test("{ meta : { name: 'Andres' } }") {
    parsesTo[Expression](mapOf(("meta", mapOf(("name", literal("Andres"))))))
  }

  test("{ }") {
    parsesTo[Expression](mapOf())
  }

  test("map.key1.key2.key3") {
    parsesTo[Expression](prop(prop(prop("map", "key1"), "key2"), "key3"))
  }

  test("({ key: 'value' }).key") {
    parsesTo[Expression](prop(mapOf(("key", literal("value"))), "key"))
  }

  test("({ inner1: { inner2: 'Value' } }).key") {
    parsesTo[Expression](prop(mapOf(("inner1", mapOf(("inner2", literal("Value"))))), "key"))
  }
}
