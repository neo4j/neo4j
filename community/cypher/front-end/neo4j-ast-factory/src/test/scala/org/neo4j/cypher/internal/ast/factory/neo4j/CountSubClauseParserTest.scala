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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.CountExpression
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition

class CountSubClauseParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statement

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[r]->(p) } > 1
      |RETURN m""".stripMargin
  ) {

    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(Some(Variable("r")(InputPosition(29, 2, 20))), None, None, None, None, OUTGOING)(
          InputPosition(27, 2, 18)
        ),
        nodePat(Some("p"))
      )(InputPosition(27, 2, 18)),
      None
    )(InputPosition(16, 2, 7), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countSubClause, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[]->() } > 1
      |RETURN m""".stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(27, 2, 18)),
        nodePat(None)
      )(InputPosition(27, 2, 18)),
      None
    )(InputPosition(16, 2, 7), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countSubClause, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m) } > 1
      |RETURN m""".stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(nodePat(Some("m")), None)(InputPosition(16, 2, 7), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countSubClause, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test(
    """MATCH (m)
      |WHERE COUNT { (m) WHERE m.prop = 3 } > 1
      |RETURN m""".stripMargin
  ) {
    val countSubClause: CountExpression =
      CountExpression(nodePat(Some("m")), Some(propEquality("m", "prop", 3)))(InputPosition(16, 2, 7), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countSubClause, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  // COUNT in a RETURN statement
  test(
    """MATCH (m)
      |RETURN COUNT { (m)-[]->() }""".stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(28, 2, 19)),
        nodePat(None)
      )(InputPosition(28, 2, 19)),
      None
    )(InputPosition(17, 2, 8), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m"))),
        return_(returnItem(countSubClause, "COUNT { (m)-[]->() }", InputPosition(17, 2, 8)))
      )
    }
  }

  // COUNT in a SET statement
  test(
    """MATCH (m)
      |SET m.howMany = COUNT { (m)-[]->() }
    """.stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(37, 2, 28)),
        nodePat(None)
      )(InputPosition(37, 2, 28)),
      None
    )(InputPosition(26, 2, 17), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m"))),
        set_(Seq(setPropertyItem("m", "howMany", countSubClause)))
      )
    }
  }

  // COUNT in a WHEN statement
  test(
    """MATCH (m)
      |RETURN CASE WHEN COUNT { (m)-[]->() } > 0 THEN "hasProperty" ELSE "other" END
    """.stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(38, 2, 29)),
        nodePat(None)
      )(InputPosition(38, 2, 29)),
      None
    )(InputPosition(27, 2, 18), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m"))),
        return_(UnaliasedReturnItem(
          CaseExpression(None, List((gt(countSubClause, literal(0)), literal("hasProperty"))), Some(literal("other")))(
            pos
          ),
          "CASE WHEN COUNT { (m)-[]->() } > 0 THEN \"hasProperty\" ELSE \"other\" END"
        )(pos))
      )
    }
  }

  // COUNT in a WITH statement
  test(
    """WITH COUNT { (m)-[]->() } AS result RETURN result""".stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      RelationshipChain(
        nodePat(Some("m")),
        RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(16, 1, 17)),
        nodePat(None)
      )(InputPosition(16, 1, 17)),
      None
    )(InputPosition(5, 1, 6), Set.empty)

    givesIncludingPositions {
      query(
        with_(AliasedReturnItem(countSubClause, Variable("result")(pos))(pos, isAutoAliased = false)),
        return_(UnaliasedReturnItem(Variable("result")(pos), "result")(pos))
      )
    }
  }

  // This would parse but would not pass the semantic check
  test(
    """MATCH (a) WHERE COUNT{(a: Label)} > 1 RETURN a""".stripMargin
  ) {
    val countSubClause: CountExpression = CountExpression(
      nodePat(Some("a"), Some(LabelExpression.Leaf(LabelName("Label")(InputPosition(26, 1, 27))))),
      None
    )(InputPosition(16, 1, 17), Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("a")), Some(where(gt(countSubClause, literal(1))))),
        return_(variableReturnItem("a"))
      )
    }
  }

  test(
    """MATCH (a), (b)
      |WHERE COUNT{(a)-[:FOLLOWS]->(b), (a)<-[:FOLLOWS]-(b)} > 0
      |RETURN a, b
      |""".stripMargin
  ) {
    failsToParse
  }

  test(
    """MATCH (a) WHERE COUNT{pt = (a)-[]->(b)} > 1 RETURN a""".stripMargin
  ) {
    failsToParse
  }
}
