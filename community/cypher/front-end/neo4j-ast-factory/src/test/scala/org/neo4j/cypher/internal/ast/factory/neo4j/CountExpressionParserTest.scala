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
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

class CountExpressionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[r]->(p) } = 4
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(25, 2, 16), position = InputPosition(24, 2, 15)),
            relPat(Some("r"), namePos = InputPosition(29, 2, 20), position = InputPosition(27, 2, 18)),
            nodePat(Some("p"), namePos = InputPosition(34, 2, 25), position = InputPosition(33, 2, 24))
          )
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(eq(countExpression, literal(4))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[]->() } < 5
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(25, 2, 16), position = InputPosition(24, 2, 15)),
            relPat(position = InputPosition(27, 2, 18)),
            nodePat(None, position = InputPosition(32, 2, 23))
          )
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(lt(countExpression, literal(5))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m) } > 7
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(Some("m"), namePos = InputPosition(25, 2, 16), position = InputPosition(24, 2, 15))
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(gt(countExpression, literal(7))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test(
    """MATCH (m)
      |WHERE COUNT { (m) WHERE m.prop = 3 } = 1
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(Some("m"), namePos = InputPosition(25, 2, 16), position = InputPosition(24, 2, 15)),
          where = Some(where(propEquality("m", "prop", 3)))
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(eq(countExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // COUNT in a RETURN statement

  test(
    """MATCH (m)
      |RETURN COUNT { (m)-[]->() }""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(26, 2, 17), position = InputPosition(25, 2, 16)),
            relPat(position = InputPosition(28, 2, 19)),
            nodePat(None, position = InputPosition(33, 2, 24))
          )
        )
      )
    )(InputPosition(17, 2, 8), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m"))),
        return_(returnItem(countExpression, "COUNT { (m)-[]->() }", InputPosition(17, 2, 8)))
      )
    }
  }

  // COUNT in a SET statement
  test(
    """MATCH (m)
      |SET m.howMany = COUNT { (m)-[]->() }
    """.stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(35, 2, 26), position = InputPosition(34, 2, 25)),
            relPat(position = InputPosition(37, 2, 28)),
            nodePat(None, position = InputPosition(42, 2, 33))
          )
        )
      )
    )(InputPosition(26, 2, 17), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m"))),
        set_(Seq(setPropertyItem("m", "howMany", countExpression)))
      )
    }
  }

  // COUNT in a WHEN statement
  test(
    """MATCH (m)
      |RETURN CASE WHEN COUNT { (m)-[]->() } > 4 THEN "hasProperty" ELSE "other" END
    """.stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(36, 2, 27), position = InputPosition(35, 2, 26)),
            relPat(position = InputPosition(38, 2, 29)),
            nodePat(None, position = InputPosition(43, 2, 34))
          )
        )
      )
    )(InputPosition(27, 2, 18), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m"))),
        return_(UnaliasedReturnItem(
          CaseExpression(None, List((gt(countExpression, literal(4)), literal("hasProperty"))), Some(literal("other")))(
            pos
          ),
          "CASE WHEN COUNT { (m)-[]->() } > 4 THEN \"hasProperty\" ELSE \"other\" END"
        )(pos))
      )
    }
  }

  // COUNT in a WITH statement
  test("WITH COUNT { (m)-[]->() } AS result RETURN result") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(14, 1, 15), position = InputPosition(13, 1, 14)),
            relPat(position = InputPosition(16, 1, 17)),
            nodePat(None, position = InputPosition(21, 1, 22))
          )
        )
      )
    )(InputPosition(5, 1, 6), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        with_(AliasedReturnItem(countExpression, Variable("result")(pos))(pos)),
        return_(UnaliasedReturnItem(Variable("result")(pos), "result")(pos))
      )
    }
  }

  test("MATCH (a) WHERE COUNT{(a: Label)} < 9 RETURN a") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(
            Some("a"),
            Some(labelLeaf("Label", InputPosition(26, 1, 27))),
            namePos = InputPosition(23, 1, 24),
            position = InputPosition(22, 1, 23)
          )
        )
      )
    )(InputPosition(16, 1, 17), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("a")), where = Some(where(lt(countExpression, literal(9))))),
        return_(variableReturnItem("a"))
      )
    }
  }

  test("MATCH (a) RETURN COUNT{ MATCH (a) } // Hello") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(Some("a"), namePos = InputPosition(31, 1, 32), position = InputPosition(30, 1, 31))
        )
      )
    )(InputPosition(17, 1, 18), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("a"))),
        return_(returnItem(countExpression, "COUNT{ MATCH (a) }"))
      )
    }
  }

  test(
    """MATCH (a), (b)
      |WHERE COUNT{(a)-[:FOLLOWS]->(b), (a)<-[:FOLLOWS]-(b)} > 6
      |RETURN a, b
      |""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          Seq(
            relationshipChain(
              nodePat(Some("a"), namePos = InputPosition(28, 2, 14), position = InputPosition(27, 2, 13)),
              relPat(None, Some(Leaf(relTypeName("FOLLOWS"))), None, None, None, OUTGOING),
              nodePat(Some("b"), namePos = InputPosition(44, 2, 30), position = InputPosition(43, 2, 29))
            ),
            relationshipChain(
              nodePat(Some("a"), namePos = InputPosition(49, 2, 35), position = InputPosition(48, 2, 34)),
              relPat(None, Some(Leaf(relTypeName("FOLLOWS"))), None, None, None, INCOMING),
              nodePat(Some("b"), namePos = InputPosition(65, 2, 51), position = InputPosition(64, 2, 50))
            )
          ),
          None
        )
      )
    )(InputPosition(21, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(Seq(nodePat(name = Some("a")), nodePat(name = Some("b"))), Some(where(gt(countExpression, literal(6))))),
        return_(variableReturnItem("a"), variableReturnItem("b"))
      )
    }
  }

  test("MATCH (a) WHERE COUNT { pt = (a)-[]->(b) } >= 5 RETURN a") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        Match(
          optional = false,
          matchMode = MatchMode.default(pos),
          Pattern.ForMatch(Seq(
            PatternPartWithSelector(
              PatternPart.AllPaths()(pos),
              NamedPatternPart(
                varFor("pt"),
                PatternPart(
                  relationshipChain(
                    nodePat(Some("a"), namePos = InputPosition(30, 1, 31), position = InputPosition(29, 1, 30)),
                    relPat(position = InputPosition(32, 1, 33)),
                    nodePat(Some("b"), namePos = InputPosition(38, 1, 39), position = InputPosition(37, 1, 38))
                  )
                )
              )(InputPosition(24, 1, 25))
            )
          ))(InputPosition(24, 1, 25)),
          Seq.empty,
          None
        )(pos)
      )
    )(InputPosition(16, 1, 17), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("a")), where = Some(where(gte(countExpression, literal(5))))),
        return_(variableReturnItem("a"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { MATCH (m)-[r]->(p) RETURN p } <= 2
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("m"), namePos = InputPosition(31, 2, 22), position = InputPosition(30, 2, 21)),
            relPat(Some("r"), namePos = InputPosition(35, 2, 26), position = InputPosition(33, 2, 24)),
            nodePat(Some("p"), namePos = InputPosition(40, 2, 31), position = InputPosition(39, 2, 30))
          )
        ),
        return_(variableReturnItem("p"))
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(lte(countExpression, literal(2))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { MATCH (m) RETURN m UNION MATCH (p) RETURN p } >= 3
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      UnionDistinct(
        singleQuery(
          match_(nodePat(name = Some("m"), namePos = InputPosition(31, 2, 22), position = InputPosition(30, 2, 21))),
          return_(variableReturnItem("m"))
        ),
        singleQuery(
          match_(nodePat(name = Some("p"), namePos = InputPosition(56, 2, 47), position = InputPosition(55, 2, 46))),
          return_(variableReturnItem("p"))
        )
      )(InputPosition(43, 2, 34))
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(gte(countExpression, literal(3))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { CREATE (n) } > 9
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        create(nodePat(name = Some("n"), namePos = InputPosition(32, 2, 23), position = InputPosition(31, 2, 22))),
        InputPosition(24, 2, 15)
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(greaterThan(countExpression, literal(9))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { INSERT (n) } > 9
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        insert(nodePat(name = Some("n"), namePos = InputPosition(32, 2, 23), position = InputPosition(31, 2, 22))),
        InputPosition(24, 2, 15)
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(greaterThan(countExpression, literal(9))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { MATCH (n) WHERE all(i in n.prop WHERE i = 4) RETURN n } = 1
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(name = Some("n"), namePos = InputPosition(31, 2, 22), position = InputPosition(30, 2, 21)),
          where = Some(
            where(
              AllIterablePredicate(
                FilterScope(
                  varFor("i"),
                  Some(
                    Equals(varFor("i"), SignedDecimalIntegerLiteral("4")(pos))(pos)
                  )
                )(pos),
                prop("n", "prop")
              )(pos)
            )
          )
        ),
        return_(variableReturnItem("n"))
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(eq(countExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test("RETURN COUNT { FINISH }") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        finish()
      )
    )(InputPosition(7, 1, 8), None, None)

    givesIncludingPositions[Statement] {
      singleQuery(
        return_(returnItem(countExpression, "COUNT { FINISH }"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test("RETURN COUNT { MATCH (n) FINISH }") {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(Some("n"), namePos = InputPosition(22, 1, 23), position = InputPosition(21, 1, 22))
        ),
        finish()
      )
    )(InputPosition(7, 1, 8), None, None)

    givesIncludingPositions[Statement] {
      singleQuery(
        return_(returnItem(countExpression, "COUNT { MATCH (n) FINISH }"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test(
    """MATCH (m)
      |WHERE COUNT { MATCH (n) FINISH } = 1
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      singleQuery(
        match_(
          nodePat(Some("n"), namePos = InputPosition(31, 2, 22), position = InputPosition(30, 2, 21))
        ),
        finish()
      )
    )(InputPosition(16, 2, 7), None, None)

    givesIncludingPositions[Statement] {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(eq(countExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { MATCH (b) RETURN b WHERE true } >= 1
      |RETURN m""".stripMargin
  ) {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'WHERE'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'WHERE': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or '}' (line 2, column 34 (offset: 43))
          |"WHERE COUNT { MATCH (b) RETURN b WHERE true } >= 1"
          |                                  ^""".stripMargin
      ))
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (a)-[r]->(b) WHERE a.prop = 1 RETURN r } > 1
      |RETURN m""".stripMargin
  ) {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'RETURN'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'RETURN': expected an expression or '}' (line 2, column 45 (offset: 54))
          |"WHERE COUNT { (a)-[r]->(b) WHERE a.prop = 1 RETURN r } > 1"
          |                                             ^""".stripMargin
      ))
  }
}
