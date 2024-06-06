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

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.symbols.CTAny

/**
 * Label expression in Node patterns
 */
class NodeLabelExpressionsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("(n)") {
    parsesTo[NodePattern] {
      nodePat(
        name = Some("n"),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A)") {
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
      nodePat(
        name = Some("n"),
        labelExpression = Some(labelLeaf("A", (1, 4, 3))),
        properties = Some(parameter("param", CTAny, position = (1, 6, 5))),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:A&B)") {
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunctions(
            Seq(
              labelLeaf("A", (1, 4, 3)),
              labelLeaf("B", (1, 6, 5)),
              labelLeaf("C", (1, 8, 7))
            ),
            (1, 7, 6)
          )
        ),
        namePos = (1, 2, 1),
        position = (1, 1, 0)
      )
    }
  }

  test("(n:!A&B)") {
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelConjunctions(
            Seq(
              labelLeaf("A", (1, 4, 3)),
              labelLeaf("B", (1, 7, 6)),
              labelLeaf("C", (1, 9, 8))
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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
    parsesTo[NodePattern] {
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

  test("(n:A|B|C)") {
    parsesTo {
      nodePat(
        name = Some("n"),
        labelExpression = Some(
          labelDisjunctions(Seq(
            labelLeaf("A"),
            labelLeaf("B"),
            labelLeaf("C")
          ))
        )
      )
    }
  }
}

class RelationshipTypeExpressionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("-[r:R|S]->") {
    parsesTo[RelationshipPattern] {
      relPat(
        Some("r"),
        Some(labelDisjunction(labelRelTypeLeaf("R"), labelRelTypeLeaf("S"))),
        position = (1, 1, 0)
      )
    }
  }

  test("-[r:!R|S]->") {
    parsesTo {
      relPat(
        Some("r"),
        Some(labelDisjunction(labelNegation(labelRelTypeLeaf("R")), labelRelTypeLeaf("S"))),
        position = (1, 1, 0)
      )
    }
  }

  test("--") {
    parsesTo[RelationshipPattern] {
      relPat(position = (1, 1, 0), direction = BOTH)
    }
  }

  test("-[]-") {
    parsesTo[RelationshipPattern] {
      relPat(position = (1, 1, 0), direction = BOTH)
    }
  }

  test("-[:A|B|C]-") {
    parsesTo {
      relPat(
        direction = BOTH,
        labelExpression = Some(labelDisjunctions(Seq(
          labelRelTypeLeaf("A"),
          labelRelTypeLeaf("B"),
          labelRelTypeLeaf("C")
        )))
      )
    }
  }
}

class MatchNodeLabelExpressionsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("MATCH (n) WHERE n:A&B") {
    parsesTo[Clause] {
      match_(
        nodePat(name = Some("n")),
        where = Some(where(
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
    parsesTo[Clause] {
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
        where = Some(where(
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
    parsesTo[Clause] {
      match_(
        nodePat(name = Some("n")),
        where = Some(
          where(
            labelExpressionPredicate("n", labelOrRelTypeLeaf("A"))
          )
        )
      )
    }
  }

  test("MATCH ()-[r]-() WHERE r:A|B") {
    parsesTo[Clause] {
      match_(
        RelationshipChain(
          NodePattern(None, None, None, None)(pos),
          RelationshipPattern(Some(varFor("r")), None, None, None, None, BOTH)(pos),
          NodePattern(None, None, None, None)(pos)
        )(pos),
        where = Some(
          where(
            labelExpressionPredicate("r", labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B")))
          )
        )
      )
    }
  }
}

class ExpressionLabelExpressionsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("[p = (n)<-[]-() WHERE ()<-[:A|B]-(n) | p]") {
    parsesTo[Expression] {
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
        )(None, None)),
        varFor("p")
      )(pos, None, None)
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[(a)-->(b:A|B) | b.prop]") {
    parsesTo[Expression] {
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
      )((1, 1, 0), None, None)
    }
  }

  test("[x IN [1,2,3] WHERE n:A | x]") {
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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
        )(pos))(pos))(None, None)),
        Some(varFor("x"))
      )
    }
  }

  //              000000000111111111122222222223333333333
  //              123456789012345678901234567890123456789
  test("[x IN [1,2,3] WHERE (n:A | B) | x]") {
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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

  // JavaCc lookahead fails to parse this properly
  test("RETURN [x IN [1,2,3] WHERE n:A | (b | x)]") {
    parsesIn[Statements] {
      case Cypher5JavaCc => _.withAnyFailure.withMessageStart(
          "Invalid input '(': expected \"+\" or \"-\" (line 1, column 34 (offset: 33))"
        )
      case _ => _.toAst(
          Statements(Seq(SingleQuery(Seq(Return(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(UnaliasedReturnItem(
                ListComprehension(
                  ExtractScope(
                    varFor("x"),
                    Some(LabelExpressionPredicate(
                      varFor("n"),
                      Disjunctions(Seq(
                        Leaf(LabelOrRelTypeName("A")(pos)),
                        Leaf(LabelOrRelTypeName("b")(pos)),
                        Leaf(LabelOrRelTypeName("x")(pos))
                      ))(pos)
                    )(pos)),
                    None
                  )(pos),
                  ListLiteral(Seq(literal(1), literal(2), literal(3)))(pos)
                )(pos),
                "[x IN [1,2,3] WHERE n:A | (b | x)]"
              )(pos)),
              None
            )(pos),
            None,
            None,
            None,
            Set()
          )(pos)))(pos)))
        )
    }
  }

  // JavaCc lookahead fails to parse this properly
  test("RETURN [x IN [1,2,3] WHERE n:A|B AND n:C|D | x]") {
    parsesIn[Statements] {
      case Cypher5JavaCc => _.withAnyFailure.withMessageStart("Invalid input '|'")
      case _ => _.toAst(
          Statements(Seq(SingleQuery(Seq(Return(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(UnaliasedReturnItem(
                ListComprehension(
                  ExtractScope(
                    varFor("x"),
                    Some(And(
                      LabelExpressionPredicate(
                        varFor("n"),
                        Disjunctions(Vector(Leaf(LabelOrRelTypeName("A")(pos)), Leaf(LabelOrRelTypeName("B")(pos))))(
                          pos
                        )
                      )(pos),
                      LabelExpressionPredicate(
                        varFor("n"),
                        Disjunctions(Seq(Leaf(LabelOrRelTypeName("C")(pos)), Leaf(LabelOrRelTypeName("D")(pos))))(pos)
                      )(pos)
                    )(pos)),
                    Some(varFor("x"))
                  )(pos),
                  ListLiteral(Seq(literal(1), literal(2), literal(3)))(pos)
                )(pos),
                "[x IN [1,2,3] WHERE n:A|B AND n:C|D | x]"
              )(pos)),
              None
            )(pos),
            None,
            None,
            None,
            Set()
          )(pos)))(pos)))
        )
    }
  }

  test("[x IN [1,2,3] WHERE n:(A | x) | x]") {
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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
    parsesTo[Expression] {
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
