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

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
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

class ExistsExpressionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions[Statement] {
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

    givesIncludingPositions {
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

    givesIncludingPositions[Statement] {
      singleQuery(
        match_(nodePat(name = Some("m")), where = Some(where(existsExpression))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { MATCH (b) RETURN b WHERE true }
      |RETURN m""".stripMargin
  ) {
    failsToParse[Statement]
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { (a)-[r]->(b) WHERE a.prop = 1 RETURN r }
      |RETURN m""".stripMargin
  ) {
    failsToParse[Statement]
  }
}
