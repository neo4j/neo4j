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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
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

class ExistsExpressionParserTest extends ParserSyntaxTreeBase[Cst.Statement, ast.Statement] {

  implicit private val javaccRule = JavaccRule.Statement
  implicit private val antlrRule = AntlrRule.Statement

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

    givesIncludingPositions {
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

    givesIncludingPositions {
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

    givesIncludingPositions {
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

    givesIncludingPositions {
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

    givesIncludingPositions {
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

    givesIncludingPositions {
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
    failsToParse
  }

  test(
    """MATCH (m)
      |WHERE EXISTS { (a)-[r]->(b) WHERE a.prop = 1 RETURN r }
      |RETURN m""".stripMargin
  ) {
    failsToParse
  }
}
