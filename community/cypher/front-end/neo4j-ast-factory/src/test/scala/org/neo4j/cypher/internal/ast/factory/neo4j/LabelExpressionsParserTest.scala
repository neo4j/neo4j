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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Label expression in Node patterns
 */
class NodeLabelExpressionsParserTest extends CypherFunSuite with JavaccParserAstTestBase[NodePattern]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[NodePattern] = JavaccRule.NodePattern

  test("(n)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(labelLeaf("A", (1, 4, 3))),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:A $param)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(labelLeaf("A", (1, 4, 3))),
        properties = Some(parameter("param", CTAny, (1, 6, 5))),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A&B)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunction(
            labelLeaf("A", (1, 4, 3)),
            labelLeaf("B")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A&B|C)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunction(
            labelConjunction(
              labelLeaf("A", (1, 4, 3)),
              labelLeaf("B")
            ),
            labelLeaf("C")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A|B&C)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunction(
            labelLeaf("A", (1, 4, 3)),
            labelConjunction(
              labelLeaf("B"),
              labelLeaf("C")
            )
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:!(A))") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelNegation(
            labelLeaf("A")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(:A&B)") {
    givesIncludingPositions {
      nodePat(
        labelExpression = Some(
          labelConjunction(
            labelLeaf("A", (1, 3, 2)),
            labelLeaf("B", (1, 5, 4))
          )
        ),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A|B)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunction(
            labelLeaf("A", (1, 4, 3)),
            labelLeaf("B")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:!A)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression =
          Some(
            labelNegation(
              labelLeaf("A")
            )
          ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A&B&C)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunction(
            labelConjunction(
              labelLeaf("A", (1, 4, 3)),
              labelLeaf("B", (1, 6, 5)),
              (1, 5, 4)
            ),
            labelLeaf("C", (1, 8, 7)),
            (1, 7, 6)
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:!A&B)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunction(
            labelNegation(labelLeaf("A")),
            labelLeaf("B")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:A&(B&C))") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunction(
            labelLeaf("A", (1, 4, 3)),
            labelConjunction(
              labelLeaf("B", (1, 7, 6)),
              labelLeaf("C", (1, 9, 8)),
              (1, 8, 7)
            ),
            (1, 5, 4)
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:!(A&B))") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelNegation(
            labelConjunction(
              labelLeaf("A"),
              labelLeaf("B")
            )
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:(A&B)|C)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunction(
            labelConjunction(
              labelLeaf("A", (1, 5, 4)),
              labelLeaf("B")
            ),
            labelLeaf("C")
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:%)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(labelWildcard((1, 4, 3))),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:!%&%)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunction(
            labelNegation(labelWildcard((1, 5, 4)), (1, 4, 3)),
            labelWildcard((1, 7, 6)),
            (1, 6, 5)
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n WHERE n:A&B)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = None,
        predicates = Some(labelExpressionPredicate(
          "n",
          labelConjunction(
            labelOrRelTypeLeaf("A", (1, 12, 11)),
            labelOrRelTypeLeaf("B", (1, 14, 13))
          )
        ))
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:A|:B)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelColonDisjunction(
            labelLeaf("A", (1, 4, 3)),
            labelLeaf("B", (1, 7, 6)),
            (1, 5, 4)
          )
        )
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("(n:A|:B:C|:!D&E|!F)") {
    givesIncludingPositions {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunction(
            labelColonDisjunction(
              labelColonDisjunction(
                labelLeaf("A", (1, 4, 3)),
                labelColonConjunction(
                  labelLeaf("B", (1, 7, 6)),
                  labelLeaf("C", (1, 9, 8)),
                  (1, 8, 7)
                )
              ),
              labelConjunction(
                labelNegation(
                  labelLeaf("D", (1, 13, 12)),
                  (1, 12, 11)
                ),
                labelLeaf("E", (1, 15, 14)),
                (1, 14, 13)
              ),
              (1, 10, 9)
            ),
            labelNegation(
              labelLeaf("F", (1, 18, 17)),
              (1, 17, 16)
            ),
            (1, 16, 15)
          )
        ),
        position = (1, 1, 0)
      )
    }
  }
}

class RelationshipTypeExpressionParserTest extends CypherFunSuite with JavaccParserAstTestBase[RelationshipPattern]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[RelationshipPattern] = JavaccRule.RelationshipPattern

  test("-[r:R|S]->") {
    givesIncludingPositions {
      relPat(
        Some("r"),
        Some(labelDisjunction(labelRelTypeLeaf("R"), labelRelTypeLeaf("S"))),
        position = (1, 1, 0)
      )
    }
  }

  test("-[r:!R|S]->") {
    givesIncludingPositions {
      relPat(
        Some("r"),
        Some(labelDisjunction(labelNegation(labelRelTypeLeaf("R")), labelRelTypeLeaf("S"))),
        position = (1, 1, 0)
      )
    }
  }

  test("--") {
    givesIncludingPositions {
      relPat(position = (1, 1, 0), direction = BOTH)
    }
  }

  test("-[]-") {
    givesIncludingPositions {
      relPat(position = (1, 1, 0), direction = BOTH)
    }
  }
}

class MatchNodeLabelExpressionsParserTest extends CypherFunSuite with JavaccParserAstTestBase[ast.Clause]
    with AstConstructionTestSupport {

  implicit val parser: JavaccRule[ast.Clause] = JavaccRule.Clause

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("MATCH (n) WHERE n:A&B") {
    givesIncludingPositions {
      match_(
        nodePat(name = Some("n")),
        Some(where(
          labelExpressionPredicate(
            "n",
            labelConjunction(
              labelOrRelTypeLeaf("A", (1, 19, 18)),
              labelOrRelTypeLeaf("B", (1, 21, 20))
            )
          )
        ))
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("MATCH (n:A|B) WHERE n:A&C") {
    givesIncludingPositions {
      match_(
        nodePat(
          name = Some("n"),
          labelExpression = Some(
            labelDisjunction(
              labelLeaf("A", (1, 10, 9)),
              labelLeaf("B", (1, 12, 11))
            )
          ),
          namePos = (1, 8, 7)
        ),
        Some(where(
          labelExpressionPredicate(
            "n",
            labelConjunction(
              labelOrRelTypeLeaf("A", (1, 23, 22)),
              labelOrRelTypeLeaf("C", (1, 25, 24)),
              (1, 24, 23)
            )
          )
        ))
      )
    }
  }

  test("MATCH (n) WHERE n:A") {
    givesIncludingPositions {
      match_(
        nodePat(name = Some("n")),
        Some(
          where(
            labelExpressionPredicate("n", labelOrRelTypeLeaf("A"))
          )
        )
      )
    }
  }

  test("MATCH ()-[r]-() WHERE r:A|B") {
    givesIncludingPositions {
      match_(
        RelationshipChain(
          NodePattern(None, None, None, None)(pos),
          RelationshipPattern(Some(varFor("r")), None, None, None, None, BOTH)(pos),
          NodePattern(None, None, None, None)(pos)
        )(pos),
        Some(
          where(
            labelExpressionPredicate("r", labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B")))
          )
        )
      )
    }
  }
}

class ExpressionLabelExpressionsParserTest extends CypherFunSuite with JavaccParserAstTestBase[Expression]
    with AstConstructionTestSupport {
  implicit val parser: JavaccRule[Expression] = JavaccRule.Expression

  test("[p = (n)<-[]-() WHERE ()<-[:A|B]-(n) | p]") {
    gives {
      PatternComprehension(
        Some(varFor("p")),
        RelationshipsPattern(
          RelationshipChain(
            nodePat(Some("n")),
            RelationshipPattern(None, None, None, None, None, INCOMING)(pos),
            nodePat()
          )(pos)
        )(pos),
        Some(PatternExpression(
          RelationshipsPattern(
            RelationshipChain(
              nodePat(),
              RelationshipPattern(
                None,
                Some(labelDisjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B"))),
                None,
                None,
                None,
                INCOMING
              )(pos),
              nodePat(Some("n"))
            )(pos)
          )(pos)
        )(Set.empty, "", "")),
        varFor("p")
      )(pos, Set.empty, "", "")
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[(a)-->(b:A|B) | b.prop]") {
    givesIncludingPositions {
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(
          RelationshipChain(
            nodePat(Some("a")),
            RelationshipPattern(None, None, None, None, None, OUTGOING)((1, 5, 4)),
            nodePat(
              Some("b"),
              Some(labelDisjunction(
                labelLeaf("A", (1, 11, 10)),
                labelLeaf("B", (1, 13, 12)),
                (1, 12, 11)
              ))
            )
          )((1, 2, 1))
        )((1, 2, 1)),
        predicate = None,
        projection = prop("b", "prop")
      )((1, 1, 0), Set.empty, "  UNNAMED0", "  UNNAMED1")
    }
  }

  test("[x IN [1,2,3] WHERE n:A | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(labelExpressionPredicate(varFor("n", position = (1, 21, 20)), labelOrRelTypeLeaf("A", (1, 23, 22)))),
        Some(varFor("x"))
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[x IN [1,2,3] WHERE (n:A|B)--() | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(PatternExpression(RelationshipsPattern(RelationshipChain(
          NodePattern(
            Some(varFor("n", position = (1, 22, 21))),
            Some(labelDisjunction(
              labelLeaf("A", (1, 24, 23)),
              labelLeaf("B", (1, 26, 25)),
              (1, 25, 24)
            )),
            None,
            None
          )(pos),
          RelationshipPattern(None, None, None, None, None, BOTH)(pos),
          NodePattern(None, None, None, None)(pos)
        )(pos))(pos))(Set.empty, "", "")),
        Some(varFor("x"))
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[x IN [1,2,3] WHERE (n:A | B) | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(labelExpressionPredicate(
          varFor("n", position = (1, 22, 21)),
          labelDisjunction(
            labelOrRelTypeLeaf("A", (1, 24, 23)),
            labelOrRelTypeLeaf("B", (1, 28, 27)),
            (1, 26, 25)
          )
        )),
        Some(varFor("x"))
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:(A | x)]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(
          labelExpressionPredicate(
            varFor("n", position = (1, 21, 20)),
            labelDisjunction(
              labelOrRelTypeLeaf("A", (1, 24, 23)),
              labelOrRelTypeLeaf("x", (1, 28, 27)),
              (1, 26, 25)
            )
          )
        ),
        None
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:A&x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(
          labelExpressionPredicate(
            varFor("n", position = (1, 21, 20)),
            labelConjunction(
              labelOrRelTypeLeaf("A", (1, 23, 22)),
              labelOrRelTypeLeaf("x", (1, 25, 24))
            )
          )
        ),
        None
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:A & (b | x)]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(labelExpressionPredicate(
          varFor("n", position = (1, 21, 20)),
          labelConjunction(
            labelOrRelTypeLeaf("A", (1, 23, 22)),
            labelDisjunction(
              labelOrRelTypeLeaf("b", (1, 28, 27)),
              labelOrRelTypeLeaf("x", (1, 32, 31)),
              (1, 30, 29)
            )
          )
        )),
        None
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:A | (b | x)]") {
    failsToParse
  }

  test("[x IN [1,2,3] WHERE n:(A | x) | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(
          labelExpressionPredicate(
            varFor("n", position = (1, 21, 20)),
            labelDisjunction(
              labelOrRelTypeLeaf("A", (1, 24, 23)),
              labelOrRelTypeLeaf("x", (1, 28, 27)),
              (1, 26, 25)
            )
          )
        ),
        Some(varFor("x"))
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:A | x | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(labelExpressionPredicate(
          varFor("n", position = (1, 21, 20)),
          labelDisjunction(
            labelOrRelTypeLeaf("A", (1, 23, 22)),
            labelOrRelTypeLeaf("x", (1, 27, 26)),
            (1, 25, 24)
          )
        )),
        Some(varFor("x"))
      )
    }
  }

  test("[x IN [1,2,3] WHERE n:A|:B:C|:!D&E|!F | x]") {
    givesIncludingPositions {
      listComprehension(
        varFor("x"),
        listOfInt(1, 2, 3),
        Some(labelExpressionPredicate(
          varFor("n"),
          labelDisjunction(
            labelColonDisjunction(
              labelColonDisjunction(
                labelOrRelTypeLeaf("A", (1, 23, 22)),
                labelColonConjunction(
                  labelOrRelTypeLeaf("B", (1, 26, 25)),
                  labelOrRelTypeLeaf("C", (1, 28, 27)),
                  (1, 27, 26)
                ),
                (1, 24, 23)
              ),
              labelConjunction(
                labelNegation(
                  labelOrRelTypeLeaf("D", (1, 32, 31)),
                  (1, 31, 30)
                ),
                labelOrRelTypeLeaf("E", (1, 34, 33)),
                (1, 33, 32)
              ),
              (1, 29, 28)
            ),
            labelNegation(
              labelOrRelTypeLeaf("F", (1, 37, 36)),
              (1, 36, 35)
            ),
            (1, 35, 34)
          )
        )),
        Some(varFor("x"))
      )
    }
  }
}
