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
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
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
        labelExpression = Some(labelAtom("A", (1, 4, 3))),
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
        labelExpression = Some(labelAtom("A", (1, 4, 3))),
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
            labelAtom("A", (1, 4, 3)),
            labelAtom("B")
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
              labelAtom("A", (1, 4, 3)),
              labelAtom("B")
            ),
            labelAtom("C")
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
            labelAtom("A", (1, 4, 3)),
            labelConjunction(
              labelAtom("B"),
              labelAtom("C")
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
            labelAtom("A")
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
            labelAtom("A", (1, 3, 2)),
            labelAtom("B", (1, 5, 4))
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
            labelAtom("A", (1, 4, 3)),
            labelAtom("B")
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
              labelAtom("A")
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
              labelAtom("A", (1, 4, 3)),
              labelAtom("B", (1, 6, 5)),
              (1, 5, 4)
            ),
            labelAtom("C", (1, 8, 7)),
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
            labelNegation(labelAtom("A")),
            labelAtom("B")
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
            labelAtom("A", (1, 4, 3)),
            labelConjunction(
              labelAtom("B", (1, 7, 6)),
              labelAtom("C", (1, 9, 8)),
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
              labelAtom("A"),
              labelAtom("B")
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
              labelAtom("A", (1, 5, 4)),
              labelAtom("B")
            ),
            labelAtom("C")
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
            labelAtom("A", (1, 12, 11)),
            labelAtom("B", (1, 14, 13))
          )
        ))
      )
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
              labelAtom("A", (1, 19, 18)),
              labelAtom("B", (1, 21, 20))
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
              labelAtom("A", (1, 10, 9)),
              labelAtom("B", (1, 12, 11))
            )
          ),
          namePos = (1, 8, 7)
        ),
        Some(where(
          labelExpressionPredicate(
            "n",
            labelConjunction(
              labelAtom("A", (1, 23, 22)),
              labelAtom("C", (1, 25, 24)),
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
            labelExpressionPredicate("n", labelAtom("A"))
          )
        )
      )
    }
  }

}

class ExpressionLabelExpressionsParserTest extends CypherFunSuite with JavaccParserAstTestBase[Expression]
    with AstConstructionTestSupport {
  implicit val parser: JavaccRule[Expression] = JavaccRule.Expression

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[(a)-->(b:A|B) | b.prop]") {
    givesIncludingPositions {
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(
          RelationshipChain(
            nodePat(Some("a")),
            RelationshipPattern(None, List(), None, None, None, OUTGOING)((1, 5, 4)),
            nodePat(
              Some("b"),
              Some(labelDisjunction(
                labelAtom("A", (1, 11, 10)),
                labelAtom("B", (1, 13, 12)),
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
        Some(labelExpressionPredicate(varFor("n", position = (1, 21, 20)), labelAtom("A", (1, 23, 22)))),
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
              labelAtom("A", (1, 24, 23)),
              labelAtom("x", (1, 28, 27)),
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
              labelAtom("A", (1, 23, 22)),
              labelAtom("x", (1, 25, 24))
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
            labelAtom("A", (1, 23, 22)),
            labelDisjunction(
              labelAtom("b", (1, 28, 27)),
              labelAtom("x", (1, 32, 31)),
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
              labelAtom("A", (1, 24, 23)),
              labelAtom("x", (1, 28, 27)),
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
            labelAtom("A", (1, 23, 22)),
            labelAtom("x", (1, 27, 26)),
            (1, 25, 24)
          )
        )),
        Some(varFor("x"))
      )
    }
  }
}
