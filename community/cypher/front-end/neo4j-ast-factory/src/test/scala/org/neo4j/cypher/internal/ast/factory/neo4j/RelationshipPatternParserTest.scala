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

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.symbols.CTAny

class RelationshipPatternParserTest extends PatternParserTestBase {

  private val pathLength = Seq(("", None), ("*1..5", Some(Some(range(Some(1), Some(5))))))

  test("MATCH ()--()") {
    gives(
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
    gives(
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
    gives(
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
    gives(
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
    gives(
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
      gives(
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
      gives(
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
      gives(
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
    failsToParse
  }

  test("MATCH ()->()") {
    failsToParse
  }

  test("MATCH ()[]->()") {
    failsToParse
  }

  test("MATCH ()-[]>()") {
    failsToParse
  }

  test("MATCH ()-]->()") {
    failsToParse
  }

  test("MATCH ()-[->()") {
    failsToParse
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
        gives(
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
        gives(
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

        gives(
          singleQuery(
            match_(
              pattern = nodePat(Some("n")),
              Some(where(existsExpression))
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

        gives(
          singleQuery(
            match_(
              pattern = nodePat(Some("n")),
              Some(where(eq(countExpression, literalInt(1))))
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

        gives(
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

      // CREATE + MERGE, these should parse but will be disallowed in semantic checking,
      // in a similar fashion as the label expressions

      test(s"CREATE ()-[$maybeVariable $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
        gives(
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
        gives(
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
        gives(
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
        gives(
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
      gives(
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(Some("r")),
              nodePat()
            ),
            Some(where(labelExpressionPredicate("r", exprAstBoth)))
          )
        )
      )
    }

    test(s"MATCH ()-[r]->() WHERE r.prop = 1 AND r $expr") {
      gives(
        singleQuery(
          match_(
            relationshipChain(
              nodePat(),
              relPat(Some("r")),
              nodePat()
            ),
            Some(where(
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
      gives(
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
    gives(
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
    gives(
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
    gives(
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
    gives(
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
    gives(
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

  test(s"MATCH ()-[r IS NULL WHERE r IS NULL]->()") {
    gives(
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
    gives(
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

  test(s"MATCH ()-[r IS NOT NULL WHERE r IS NOT NULL]->()") {
    failsToParse
  }
}
