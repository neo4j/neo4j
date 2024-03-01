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

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.symbols.CTAny

class RelationshipPatternParserTest extends PatternParserTestBase {
  private val pathLength = Seq(("", None), ("*1..5", Some(Some(range(Some(1), Some(5))))))

  test("MATCH ()--()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(direction = BOTH),
            nodePat()
          )
        )
      )
    )
  }

  test("MATCH ()-->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(direction = OUTGOING),
            nodePat()
          )
        )
      )
    )
  }

  test("MATCH ()<--()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(direction = INCOMING),
            nodePat()
          )
        )
      )
    )
  }

  test("MATCH ()<-->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(direction = BOTH),
            nodePat()
          )
        )
      )
    )
  }

  test("MATCH ()-[$props]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              properties = Some(ExplicitParameter("props", CTAny)(pos))
            ),
            nodePat()
          )
        )
      )
    )
  }

  for {
    (maybeVariable, maybeVariableAst) <- variable
    (maybePathLength, maybePathLengthAst) <- pathLength
    (maybeProperties, maybePropertiesAst) <- properties
  } yield {
    test(s"MATCH ()-[$maybeVariable$maybePathLength$maybeProperties]-()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                name = maybeVariableAst,
                direction = BOTH,
                length = maybePathLengthAst,
                properties = maybePropertiesAst
              ),
              nodePat()
            )
          )
        )
      )
    }

    test(s"MATCH ()-[$maybeVariable$maybePathLength$maybeProperties]->()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                name = maybeVariableAst,
                direction = OUTGOING,
                length = maybePathLengthAst,
                properties = maybePropertiesAst
              ),
              nodePat()
            )
          )
        )
      )
    }

    test(s"MATCH ()<-[$maybeVariable$maybePathLength$maybeProperties]-()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                name = maybeVariableAst,
                direction = INCOMING,
                length = maybePathLengthAst,
                properties = maybePropertiesAst
              ),
              nodePat()
            )
          )
        )
      )
    }
  }

  test("MATCH ()-()") {
    failsToParse[Statements]()
  }

  test("MATCH ()->()") {
    failsToParse[Statements]()
  }

  test("MATCH ()[]->()") {
    failsToParse[Statements]()
  }

  test("MATCH ()-[]>()") {
    failsToParse[Statements]()
  }

  test("MATCH ()-]->()") {
    failsToParse[Statements]()
  }

  test("MATCH ()-[->()") {
    failsToParse[Statements]()
  }

  for {
    (expr, _, exprAstRel, exprAstBoth) <- labelExpressions
  } yield {
    for {
      (maybeVariable, maybeVariableAst) <- variable
      (maybePathLength, maybePathLengthAst) <- pathLength
      (maybeProperties, maybePropertiesAst) <- properties
      (maybeWhere, maybeWhereAst) <- where
    } yield {
      // MATCH
      test(s"MATCH ()-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
        gives[Statements](
          singleQuery(
            match_(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )
      }

      // OPTIONAL MATCH
      test(
        s"OPTIONAL MATCH ()-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()"
      ) {
        gives[Statements](
          singleQuery(
            optionalMatch(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )
      }

      // EXISTS
      test(s"MATCH (n) WHERE EXISTS {(n)-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()}") {
        val existsExpression: ExistsExpression = ExistsExpression(
          singleQuery(
            match_(
              relationshipChain(
                nodePat(Some("n")),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )(pos, None, None)

        gives[Statements](
          singleQuery(
            match_(
              pattern = nodePat(Some("n")),
              where = Some(where(existsExpression))
            )
          )
        )
      }

      // COUNT
      test(
        s"MATCH (n) WHERE COUNT {(n)-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()} = 1"
      ) {
        val countExpression: CountExpression = CountExpression(
          singleQuery(
            match_(
              relationshipChain(
                nodePat(Some("n")),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )(pos, None, None)

        gives[Statements](
          singleQuery(
            match_(
              pattern = nodePat(Some("n")),
              where = Some(where(eq(countExpression, literalInt(1))))
            )
          )
        )
      }

      // COLLECT
      test(
        s"""
           |MATCH (n)
           |RETURN COLLECT {
           |  MATCH (n)-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()
           |  RETURN 42 AS answer
           |} AS collect
           |""".stripMargin
      ) {
        val collectExpression: CollectExpression = CollectExpression(
          singleQuery(
            match_(
              relationshipChain(
                nodePat(Some("n")),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            ),
            return_(
              aliasedReturnItem(
                literalInt(42L),
                "answer"
              )
            )
          )
        )(pos, None, None)

        gives[Statements](
          singleQuery(
            match_(
              pattern = nodePat(Some("n"))
            ),
            return_(
              aliasedReturnItem(
                collectExpression,
                "collect"
              )
            )
          )
        )
      }

      // CREATE + MERGE, these should parse, but all label expressions except : and & will be disallowed in semantic checking

      test(s"CREATE ()-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
        gives[Statements](
          singleQuery(
            create(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )
      }

      test(s"MERGE ()-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
        gives[Statements](
          singleQuery(
            merge(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  maybeWhereAst
                ),
                nodePat()
              )
            )
          )
        )
      }
    }

    // Inlined WHERE

    for {
      (maybeVariable, maybeVariableAst) <- variable
      (maybePathLength, maybePathLengthAst) <- pathLength
      (maybeProperties, maybePropertiesAst) <- properties
    } yield {
      test(s"MATCH ()-[$maybeVariable $maybePathLength $maybeProperties WHERE x $expr]->()") {
        gives[Statements](
          singleQuery(
            match_(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  None,
                  maybePathLengthAst,
                  maybePropertiesAst,
                  Some(labelExpressionPredicate("x", exprAstBoth))
                ),
                nodePat()
              )
            )
          )
        )
      }

      test(s"MATCH ()-[$maybeVariable $expr $maybePathLength $maybeProperties WHERE x $expr]->()") {
        gives[Statements](
          singleQuery(
            match_(
              relationshipChain(
                nodePat(),
                relPat(
                  maybeVariableAst,
                  Some(exprAstRel),
                  maybePathLengthAst,
                  maybePropertiesAst,
                  Some(labelExpressionPredicate("x", exprAstBoth))
                ),
                nodePat()
              )
            )
          )
        )
      }
    }

    // WHERE

    test(s"MATCH ()-[r]->()  WHERE r $expr") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(Some("r")),
              nodePat()
            ),
            where = Some(where(labelExpressionPredicate("r", exprAstBoth)))
          )
        )
      )
    }

    test(s"MATCH ()-[r]->() WHERE r.prop = 1 AND r $expr") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(Some("r")),
              nodePat()
            ),
            where = Some(where(
              and(
                equals(prop("r", "prop"), literalInt(1)),
                labelExpressionPredicate("r", exprAstBoth)
              )
            ))
          )
        )
      )
    }

    // RETURN

    test(s"MATCH ()-[r]->() RETURN r $expr AS rel") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(Some("r")),
              nodePat()
            )
          ),
          return_(
            aliasedReturnItem(
              labelExpressionPredicate("r", exprAstBoth),
              "rel"
            )
          )
        )
      )
    }

  }

  // CASE
  test(
    s"""MATCH (n)-[r]->(m)
       |RETURN CASE
       |WHEN n IS A&B THEN 1
       |WHEN r IS A|B THEN 2
       |ELSE -1
       |END
       |AS value
       |""".stripMargin
  ) {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("n")),
            relPat(Some("r")),
            nodePat(Some("m"))
          )
        ),
        return_(
          aliasedReturnItem(
            caseExpression(
              None,
              Some(literalInt(-1)),
              (
                labelExpressionPredicate(
                  "n",
                  labelConjunction(
                    labelOrRelTypeLeaf("A", containsIs = true),
                    labelOrRelTypeLeaf("B", containsIs = true),
                    containsIs = true
                  )
                ),
                literalInt(1)
              ),
              (
                labelExpressionPredicate(
                  "r",
                  labelDisjunction(
                    labelOrRelTypeLeaf("A", containsIs = true),
                    labelOrRelTypeLeaf("B", containsIs = true),
                    containsIs = true
                  )
                ),
                literalInt(2)
              )
            ),
            "value"
          )
        )
      )
    )
  }

  // Edge cases

  test(s"MATCH ()-[r IS IS]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("r"),
              Some(labelRelTypeLeaf("IS", containsIs = true))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH ()-[IS:IS]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("IS"),
              Some(labelRelTypeLeaf("IS"))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH ()-[WHERE IS IS|WHERE WHERE WHERE.IS = IS.WHERE]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("WHERE"),
              labelExpression = Some(labelDisjunction(
                labelRelTypeLeaf("IS", containsIs = true),
                labelRelTypeLeaf("WHERE", containsIs = true),
                containsIs = true
              )),
              predicates = Some(equals(prop("WHERE", "IS"), prop("IS", "WHERE")))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH ()-[r WHERE r IS NULL]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("r"),
              predicates = Some(isNull(varFor("r")))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH ()-[r IS `NULL` WHERE r IS NULL]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("r"),
              labelExpression = Some(labelRelTypeLeaf("NULL", containsIs = true)),
              predicates = Some(isNull(varFor("r")))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH ()-[r WHERE r IS NOT NULL]->()") {
    gives[Statements](
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("r"),
              predicates = Some(isNotNull(varFor("r")))
            ),
            nodePat()
          )
        )
      )
    )
  }

  // Relationship types NOT, NULL and TYPED are not allowed together with IS keyword unless escaped
  for {
    label <- Seq("NOT", "NULL", "TYPED")
  } yield {
    test(s"MATCH ()-[r:$label]->()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                Some("r"),
                Some(labelRelTypeLeaf(label))
              ),
              nodePat()
            )
          )
        )
      )
    }

    test(s"MATCH ()-[r:`$label`]->()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                Some("r"),
                Some(labelRelTypeLeaf(label))
              ),
              nodePat()
            )
          )
        )
      )
    }

    test(s"MATCH ()-[r IS $label]->()") {
      failsToParse[Statements]()
    }

    test(s"MATCH ()-[r IS `$label`]->()") {
      gives[Statements](
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(
                Some("r"),
                Some(labelRelTypeLeaf(label, containsIs = true))
              ),
              nodePat()
            )
          )
        )
      )
    }
  }

  test(s"MATCH ()-[r IS NOT NULL WHERE r IS NOT NULL]->()") {
    failsToParse[Statements]()
  }

  test("(n)-[r]-(m)") {
    parsesTo[Expression](
      PatternExpression(
        RelationshipsPattern(
          RelationshipChain(
            nodePat(Some("n")),
            relPat(
              Some("r"),
              None,
              None,
              None,
              None,
              SemanticDirection.BOTH
            ),
            nodePat(Some("m"))
          )(pos)
        )(pos)
      )(None, None)
    )
  }

  test("(n)-[r]-(m)-[r1]-(m1)") {
    parsesTo[Expression](
      PatternExpression(
        RelationshipsPattern(
          RelationshipChain(
            RelationshipChain(
              nodePat(Some("n")),
              relPat(
                Some("r"),
                None,
                None,
                None,
                None,
                SemanticDirection.BOTH
              ),
              nodePat(Some("m"))
            )(pos),
            relPat(
              Some("r1"),
              None,
              None,
              None,
              None,
              SemanticDirection.BOTH
            ),
            nodePat(Some("m1"))
          )(pos)
        )(pos)
      )(None, None)
    )
  }

  test("[p = (n)<-[]-()<-[]-() WHERE ()<-[:A|B]-(n) | p]") {
    parsesTo[Expression] {
      PatternComprehension(
        Some(varFor("p")),
        RelationshipsPattern(
          RelationshipChain(
            RelationshipChain(
              nodePat(Some("n")),
              RelationshipPattern(None, None, None, None, None, INCOMING)(pos),
              nodePat()
            )(pos),
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

}
