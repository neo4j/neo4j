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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseSuccess
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.Parses
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.parser.javacc.TokenMgrException
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.SyntaxException

class MiscParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("RETURN 1 AS x //l33t comment") {
    parsesTo[Statement] {
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

    "SET map.node.property = 123" should parseTo[Clause](
      SetClause(Seq(
        SetPropertyItem(chainedProperties, literal(123))(pos)
      ))(pos)
    )

    "REMOVE map.node.property" should parseTo[Clause](
      Remove(Seq(
        RemovePropertyItem(chainedProperties)
      ))(pos)
    )
  }

  test("should allow True and False as label name") {
    "(:True)" should parseTo[NodePattern](NodePattern(None, Some(labelLeaf("True")), None, None)(pos))
    "(:False)" should parseTo[NodePattern](NodePattern(None, Some(labelLeaf("False")), None, None)(pos))

    "(t:True)" should parseTo[NodePattern](nodePat(name = Some("t"), labelExpression = Some(labelLeaf("True"))))
    "(f:False)" should parseTo[NodePattern](nodePat(name = Some("f"), labelExpression = Some(labelLeaf("False"))))
  }

  test("-[:Person*1..2]-") {
    parsesTo[RelationshipPattern] {
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
      )(pos)
    }
  }

  test("should not parse list literal as pattern comprehension") {
    val listLiterals = Seq(
      "[x = '1']",
      "[x = ()--()]",
      "[x = ()--()--()--()--()--()--()--()--()--()--()]"
    )
    for (l <- listLiterals) withClue(l) {
      l should parse[Expression].withAstLike(_ shouldBe a[ListLiteral])
    }
  }

  test("should not parse pattern comprehensions with single nodes") {
    "[p = (x) | p]" should notParse[PatternComprehension].in {
      case Cypher5JavaCc => _.withMessageStart("Encountered \" \"|\" \"|\"\" at line 1, column 10.")
      case _ => _.withSyntaxError(
          """Invalid input '|': expected '-' (line 1, column 10 (offset: 9))
            |"[p = (x) | p]"
            |          ^""".stripMargin
        )
    }
  }

  test("should handle escaping in string literals") {
    """'\\\''""" should parseTo[StringLiteral](literalString("""\'"""))
  }

  test("Normal Form is only converted to strings inside functions, else treated as a variable") {
    Seq("NFC", "NFD", "NFKC", "NFKD").foreach { normalForm =>
      s"RETURN $normalForm" should parseTo[Clause](
        return_(variableReturnItem(normalForm))
      )
    }
  }

  test("Unicode escape outside of string literals") {
    // https://neo4j.com/docs/cypher-manual/current/syntax/parsing/#_using_unicodes_in_cypher
    "M\\u0041TCH (m) RETURN m" should parse[Statement].toAstPositioned(
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
    )
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
    parsesTo[Expression] {
      sliceFull(listOf(literal(1), literal(2), literal(3), literal(4)), literal(1), literal(2))
    }
  }

  test("[1,2,3,4][1..2][2..3]") {
    parsesTo[Expression] {
      sliceFull(
        sliceFull(listOf(literal(1), literal(2), literal(3), literal(4)), literal(1), literal(2)),
        literal(2),
        literal(3)
      )
    }
  }

  test("collection[1..2]") {
    parsesTo[Expression](sliceFull(varFor("collection"), literal(1), literal(2)))
  }

  test("[1,2,3,4][2]") {
    parsesTo[Expression] {
      containerIndex(listOf(literal(1), literal(2), literal(3), literal(4)), 2)
    }
  }

  test("[[1,2]][0][6]") {
    parsesTo[Expression] {
      containerIndex(containerIndex(listOf(listOf(literal(1), literal(2))), 0), 6)
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

  test("COUNT(*)") {
    parsesTo[Expression] {
      CountStar()(pos)
    }
  }

  test("({ inner1: { inner2: 'Value' } }).key") {
    parsesTo[Expression](prop(mapOf(("inner1", mapOf(("inner2", literal("Value"))))), "key"))
  }

  test("multiple unions") {
    val q =
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS x
        |UNION
        |RETURN 3 AS x
        |""".stripMargin
    q should parseTo[Statements](
      unionDistinct(
        singleQuery(returnLit(1 -> "x")),
        singleQuery(returnLit(2 -> "x")),
        singleQuery(returnLit(3 -> "x"))
      )
    )
  }

  test("shortest query") {
    """MATCH (src:A), (dst:D)
      |RETURN shortestPath((src:A)-[*]->(dst:D)) as path
      |""".stripMargin should parseTo[Statements] {
      Statements(Seq(SingleQuery(Seq(
        Match(
          optional = false,
          DifferentRelationships(implicitlyCreated = true)(pos),
          ForMatch(Seq(
            PatternPartWithSelector(
              AllPaths()(pos),
              PathPatternPart(NodePattern(Some(varFor("src")), Some(Leaf(LabelName("A")(pos))), None, None)(pos))
            ),
            PatternPartWithSelector(
              AllPaths()(pos),
              PathPatternPart(NodePattern(Some(varFor("dst")), Some(Leaf(LabelName("D")(pos))), None, None)(pos))
            )
          ))(pos),
          Seq(),
          None
        )(pos),
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(AliasedReturnItem(
              ShortestPathExpression(ShortestPathsPatternPart(
                RelationshipChain(
                  NodePattern(Some(varFor("src")), Some(Leaf(LabelName("A")(pos))), None, None)(pos),
                  RelationshipPattern(None, None, Some(None), None, None, OUTGOING)(pos),
                  NodePattern(Some(varFor("dst")), Some(Leaf(LabelName("D")(pos))), None, None)(pos)
                )(pos),
                single = true
              )(pos)),
              varFor("path")
            )(pos)),
            None
          )(pos),
          None,
          None,
          None,
          Set(),
          addedInRewrite = false
        )(pos)
      ))(pos)))
    }
  }

  test("MATCH (a)->(b) RETURN *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => (a: Parses[Statements]) =>
          a.throws[OpenCypherExceptionFactory.SyntaxException]
            .withMessageStart("Invalid input '-': expected")
      case _ => (a: Parses[Statements]) =>
          a.throws[SyntaxException]
            .withMessage(
              """Invalid input '>': expected '-' (line 1, column 11 (offset: 10))
                |"MATCH (a)->(b) RETURN *"
                |           ^""".stripMargin
            )
    }
  }

  test("MATCH (a)--->(b) RETURN *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.throws[OpenCypherExceptionFactory.SyntaxException]
          .withMessageStart("Invalid input '-': expected")
      case _ =>
        _.throws[SyntaxException]
          .withMessage(
            """Invalid input '-': expected '(' (line 1, column 12 (offset: 11))
              |"MATCH (a)--->(b) RETURN *"
              |            ^""".stripMargin
          )
    }
  }

  test("RETURN RETURN 1") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.throws[OpenCypherExceptionFactory.SyntaxException]
          .withMessageStart("Invalid input '1': expected")
      case _ =>
        _.throws[SyntaxException]
          .withMessage(
            """Invalid input '1': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 15 (offset: 14))
              |"RETURN RETURN 1"
              |               ^""".stripMargin
          )
    }
  }

  test("RETURN 'hell") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.throws[TokenMgrException]
          .withMessageStart("Lexical error at line 1, column 13.  Encountered: <EOF> after : \"\"")
      case _ =>
        _.throws[SyntaxException]
          .withMessage(
            """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 8 (offset: 7))
              |"RETURN 'hell"
              |        ^""".stripMargin
          )
    }
  }

  test("correct positions in errors with unicode escapes and comments") {
    val query = "/* \\u003A\\u0029 */  MATCH /* */ (a)/* */->/* */(b)/* */RETURN *"
    query should notParse[Statements].in {
      case Cypher5JavaCc =>
        _.throws[OpenCypherExceptionFactory.SyntaxException]
          .withMessageStart("Invalid input '-': expected")
          .withMessageContaining("(line 1, column 41 (offset: 40))")
      case _ =>
        _.withSyntaxError(
          s"""Invalid input '>': expected '-' (line 1, column 42 (offset: 41))
             |"$query"
             |                                          ^""".stripMargin
        )
    }
  }

  test("MATCH (n) WHERE n.prop = 'ab + 1") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 26 (offset: 25))
            |"MATCH (n) WHERE n.prop = 'ab + 1"
            |                          ^""".stripMargin
        )
    }
  }

  test("MATCH (n) WHERE n.prop = 'ab'' + 1") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 30 (offset: 29))
            |"MATCH (n) WHERE n.prop = 'ab'' + 1"
            |                              ^""".stripMargin
        )
    }
  }

  test("MATCH (n) WHERE n.prop = '") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 26 (offset: 25))
            |"MATCH (n) WHERE n.prop = '"
            |                          ^""".stripMargin
        )
    }
  }

  test("MATCH (n) WHERE n.'prop") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 19 (offset: 18))
            |"MATCH (n) WHERE n.'prop"
            |                   ^""".stripMargin
        )
    }
  }

  test("SHOW SETTING 'a', 'b''") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 22 (offset: 21))
            |"SHOW SETTING 'a', 'b''"
            |                      ^""".stripMargin
        )
    }

  }

  test("MATCH (n) WHERE n.prop = 'ab\\'c' AND 'b\\'c' AND 'c\\'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at line 1, column 53")
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 49 (offset: 48))
            |"MATCH (n) WHERE n.prop = 'ab\'c' AND 'b\'c' AND 'c\'"
            |                                                 ^""".stripMargin
        )
    }
  }

  test("RETURN '\\\\'") {
    parsesTo[Statements](Statements(List(SingleQuery(List(Return(
      false,
      ReturnItems(false, List(UnaliasedReturnItem(StringLiteral("\\")(pos.withInputLength(0)), "'\\\\'")(pos)), None)(
        pos
      ),
      None,
      None,
      None,
      Set(),
      false
    )(pos)))(pos))))
  }

  test("RETURN /* abc */ 1 /*'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error at line 1, column 23.")
      case _ => _.withSyntaxError(
          """Failed to parse comment. A comment starting on `/*` must have a closing `*/`. (line 1, column 21 (offset: 20))
            |"RETURN /* abc */ 1 /*'"
            |                     ^""".stripMargin
        )
    }
  }

  test("return item text parses correctly") {
    "return 'a\\u000Ab\\u000Ac', 'x'" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        returnItem(
          StringLiteral("a\nb\nc")(InputPosition(7, 1, 8).withInputLength(17)),
          "'a\\u000Ab\\u000Ac'"
        ),
        returnItem(
          StringLiteral("x")(InputPosition(26, 1, 27).withInputLength(3)),
          "'x'"
        )
      ))))
    }

    "return 1 +  /* hello */ 2, 3" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        returnItem(
          add(
            SignedDecimalIntegerLiteral("1")(InputPosition(7, 1, 8)),
            SignedDecimalIntegerLiteral("2")(InputPosition(24, 1, 25))
          ),
          "1 +  /* hello */ 2"
        ),
        returnItem(
          SignedDecimalIntegerLiteral("3")(InputPosition(27, 1, 28)),
          "3"
        )
      ))))
    }
  }

  test("LOAD CSV FROM 'ftp://mark:Password1@localhost/images.txt' AS line RETURN line") {
    val result = parseAst[Statements](testName)
    result.result.foreach { case (parser, ast) =>
      ast match {
        case ParseSuccess(Statements(Seq(SingleQuery(Seq(loadCsv: LoadCSV, _))))) =>
          withClue(s"parser=$parser, url class=${loadCsv.urlString.getClass}") {
            loadCsv.urlString.isInstanceOf[SensitiveLiteral] shouldBe true
          }
        case other => fail(s"Unexpected ast in parser $parser:\n$other")
      }
    }
  }

  test("unicode arrow line and head") {
    "MERGE (project)–[:HAS_FOLDER]-⟩(folder)" should parse[Statements]
  }
}
