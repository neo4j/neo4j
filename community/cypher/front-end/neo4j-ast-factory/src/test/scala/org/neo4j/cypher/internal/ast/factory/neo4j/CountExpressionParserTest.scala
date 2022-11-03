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
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition

class CountExpressionParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statement

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[r]->(p) } > 1
      |RETURN m""".stripMargin
  ) {

    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(Some(Variable("r")(InputPosition(29, 2, 20))), None, None, None, None, OUTGOING)(
              InputPosition(27, 2, 18)
            ),
            nodePat(Some("p"))
          )(pos)
        )
      ))(InputPosition(24, 2, 15)),
      None
    )(InputPosition(16, 2, 7), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m)-[]->() } > 1
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(27, 2, 18)),
            nodePat(None)
          )(pos)
        )
      ))(InputPosition(24, 2, 15)),
      None
    )(InputPosition(16, 2, 7), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countExpression, literal(1))))),
        return_(variableReturnItem("m"))
      )
    }
  }

  test(
    """MATCH (m)
      |WHERE COUNT { (m) } > 1
      |RETURN m""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          nodePat(Some("m"))
        )
      ))(InputPosition(24, 2, 15)),
      None
    )(InputPosition(16, 2, 7), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countExpression, literal(1))))),
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
    val countExpression: CountExpression =
      CountExpression(
        Pattern(Seq(
          EveryPath(
            nodePat(Some("m"))
          )
        ))(InputPosition(24, 2, 15)),
        Some(propEquality("m", "prop", 3))
      )(InputPosition(16, 2, 7), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m")), Some(where(gt(countExpression, literal(1))))),
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
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(28, 2, 19)),
            nodePat(None)
          )(pos)
        )
      ))(InputPosition(25, 2, 16)),
      None
    )(InputPosition(17, 2, 8), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
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
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(37, 2, 28)),
            nodePat(None)
          )(pos)
        )
      ))(InputPosition(34, 2, 25)),
      None
    )(InputPosition(26, 2, 17), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m"))),
        set_(Seq(setPropertyItem("m", "howMany", countExpression)))
      )
    }
  }

  // COUNT in a WHEN statement
  test(
    """MATCH (m)
      |RETURN CASE WHEN COUNT { (m)-[]->() } > 0 THEN "hasProperty" ELSE "other" END
    """.stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(38, 2, 29)),
            nodePat(None)
          )(pos)
        )
      ))(InputPosition(35, 2, 26)),
      None
    )(InputPosition(27, 2, 18), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("m"))),
        return_(UnaliasedReturnItem(
          CaseExpression(None, List((gt(countExpression, literal(0)), literal("hasProperty"))), Some(literal("other")))(
            pos
          ),
          "CASE WHEN COUNT { (m)-[]->() } > 0 THEN \"hasProperty\" ELSE \"other\" END"
        )(pos))
      )
    }
  }

  // COUNT in a WITH statement
  test("WITH COUNT { (m)-[]->() } AS result RETURN result") {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("m")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)(InputPosition(16, 1, 17)),
            nodePat(None)
          )(pos)
        )
      ))(InputPosition(13, 1, 14)),
      None
    )(InputPosition(5, 1, 6), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        with_(AliasedReturnItem(countExpression, Variable("result")(pos))(pos, isAutoAliased = false)),
        return_(UnaliasedReturnItem(Variable("result")(pos), "result")(pos))
      )
    }
  }

  test("MATCH (a) WHERE COUNT{(a: Label)} > 1 RETURN a") {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          nodePat(Some("a"), Some(LabelExpression.Leaf(LabelName("Label")(InputPosition(26, 1, 27)))))
        )
      ))(InputPosition(22, 1, 23)),
      None
    )(InputPosition(16, 1, 17), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("a")), Some(where(gt(countExpression, literal(1))))),
        return_(variableReturnItem("a"))
      )
    }
  }

  test("MATCH (a) RETURN COUNT{ MATCH (a) }") {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          nodePat(Some("a"), None)
        )
      ))(InputPosition(30, 1, 31)),
      None
    )(InputPosition(17, 1, 18), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("a")), None),
        return_(returnItem(countExpression, "COUNT{ MATCH (a) }"))
      )
    }
  }

  test(
    """MATCH (a), (b)
      |WHERE COUNT{(a)-[:FOLLOWS]->(b), (a)<-[:FOLLOWS]-(b)} > 0
      |RETURN a, b
      |""".stripMargin
  ) {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        EveryPath(
          RelationshipChain(
            nodePat(Some("a")),
            relPat(None, Some(Leaf(relTypeName("FOLLOWS"))), None, None, None, OUTGOING),
            nodePat(Some("b"))
          )(pos)
        ),
        EveryPath(
          RelationshipChain(
            nodePat(Some("a")),
            relPat(None, Some(Leaf(relTypeName("FOLLOWS"))), None, None, None, INCOMING),
            nodePat(Some("b"))
          )(pos)
        )
      ))(InputPosition(27, 2, 13)),
      None
    )(InputPosition(21, 2, 7), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(Seq(nodePat(name = Some("a")), nodePat(name = Some("b"))), Some(where(gt(countExpression, literal(0))))),
        return_(variableReturnItem("a"), variableReturnItem("b"))
      )
    }
  }

  test("MATCH (a) WHERE COUNT { pt = (a)-[]->(b) } > 1 RETURN a") {
    val countExpression: CountExpression = CountExpression(
      Pattern(Seq(
        NamedPatternPart(
          varFor("pt"),
          EveryPath(
            RelationshipChain(
              nodePat(Some("a")),
              relPat(),
              nodePat(Some("b"))
            )(pos)
          )
        )(InputPosition(24, 1, 25))
      ))(InputPosition(24, 1, 25)),
      None
    )(InputPosition(16, 1, 17), Set.empty, Set.empty)

    givesIncludingPositions {
      query(
        match_(nodePat(name = Some("a")), Some(where(gt(countExpression, literal(1))))),
        return_(variableReturnItem("a"))
      )
    }
  }
}
