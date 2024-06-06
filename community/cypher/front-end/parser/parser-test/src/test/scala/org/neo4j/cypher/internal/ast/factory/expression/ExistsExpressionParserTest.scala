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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition

class ExistsExpressionParserTest extends AstParsingTestBase {

  test(
    """MATCH (m)
      |WHERE EXISTS { (m)-[r]->(p) }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(Some(Variable("r")(InputPosition(30, 2, 21))), None, None, None, None, OUTGOING)(
              InputPosition(28, 2, 19)
            ),
            nodePat(Some("p"))
          )(pos)
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (m)-[r]->(p) WHERE p.a > 5 }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(Some(Variable("r")(InputPosition(36, 2, 27))), None, None, None, None, OUTGOING)(
              InputPosition(34, 2, 25)
            ),
            nodePat(Some("p"))
          )(pos),
          where = Some(where(greaterThan(prop(Variable("p")(pos), "a"), literal(5))))
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (m)-[r]->(p) }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(Some(Variable("r")(InputPosition(36, 2, 27))), None, None, None, None, OUTGOING)(
              InputPosition(34, 2, 25)
            ),
            nodePat(Some("p"))
          )(pos)
        )
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (m)-[r]->(p) RETURN p }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(Some(Variable("r")(InputPosition(36, 2, 27))), None, None, None, None, OUTGOING)(
              InputPosition(34, 2, 25)
            ),
            nodePat(Some("p"))
          )(pos)
        ),
        return_(variableReturnItem("p"))
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (m) RETURN m UNION MATCH (p) RETURN p }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      UnionDistinct(
        singleQuery(
          match_(nodePat(name = Some("m"), None)),
          return_(variableReturnItem("m"))
        ),
        singleQuery(
          match_(nodePat(name = Some("p"), None)),
          return_(variableReturnItem("p"))
        )
      )(InputPosition(44, 2, 35))
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { CREATE (n) }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        create(nodePat(name = Some("n"))),
        InputPosition(25, 2, 16)
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { INSERT (n) }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        insert(nodePat(name = Some("n"))),
        InputPosition(25, 2, 16)
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (n) WHERE all(i in n.prop WHERE i = 4) RETURN n }
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          nodePat(name = Some("n")),
          where = Some(
            where(
              AllIterablePredicate(
                FilterScope(
                  varFor("i"),
                  Some(
                    Equals(varFor("i"), SignedDecimalIntegerLiteral("4")(pos))(pos)
                  )
                )(pos),
                Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)
              )(pos)
            )
          )
        ),
        return_(variableReturnItem("n"))
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test("RETURN EXISTS { FINISH }") {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        finish()
      )
    )(InputPosition(7, 1, 8), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        return_(returnItem(existsExpression, "EXISTS { FINISH }"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test("RETURN EXISTS { MATCH (n) FINISH }") {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          nodePat(Some("n"), namePos = InputPosition(23, 1, 24), position = InputPosition(22, 1, 23))
        ),
        finish()
      )
    )(InputPosition(7, 1, 8), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        return_(returnItem(existsExpression, "EXISTS { MATCH (n) FINISH }"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (n) FINISH } = 1
      |RETURN m""".stripMargin
  ) {
    val existsExpression: ExistsExpression = ExistsExpression(
      singleQuery(
        match_(
          nodePat(Some("n"), namePos = InputPosition(32, 2, 23), position = InputPosition(31, 2, 22))
        ),
        finish()
      )
    )(InputPosition(16, 2, 7), None, None)

    parses[Statement].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(equals(existsExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (b) RETURN b WHERE true }
      |RETURN m""".stripMargin
  ) {
    failsParsing[Statements].withMessageStart("Invalid input 'WHERE'")
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { (a)-[r]->(b) WHERE a.prop = 1 RETURN r }
      |RETURN m""".stripMargin
  ) {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'RETURN'")
      case _ => _.withSyntaxError(
          """Invalid input 'RETURN': expected an expression or '}' (line 2, column 46 (offset: 55))
            |"WHERE EXISTS { (a)-[r]->(b) WHERE a.prop = 1 RETURN r }"
            |                                              ^""".stripMargin
        )
    }
  }
}
