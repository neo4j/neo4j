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

import org.neo4j.cypher.internal.ast.Statement

class IsParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statements

  private val labelExpressions = Seq(
    (
      "A",
      labelLeaf("A"),
      labelRelTypeLeaf("A"),
      labelOrRelTypeLeaf("A")
    ),
    (
      "A&B",
      labelConjunction(labelLeaf("A"), labelLeaf("B")),
      labelConjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")),
      labelConjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))
    ),
    (
      "A|B",
      labelDisjunction(labelLeaf("A"), labelLeaf("B")),
      labelDisjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")),
      labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))
    ),
    (
      "%",
      labelWildcard(),
      labelWildcard(),
      labelWildcard()
    ),
    (
      "!A",
      labelNegation(labelLeaf("A")),
      labelNegation(labelRelTypeLeaf("A")),
      labelNegation(labelOrRelTypeLeaf("A"))
    ),
    (
      "!(A|B)",
      labelNegation(labelDisjunction(labelLeaf("A"), labelLeaf("B"))),
      labelNegation(labelDisjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B"))),
      labelNegation(labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B")))
    ),
    (
      "A&!B",
      labelConjunction(labelLeaf("A"), labelNegation(labelLeaf("B"))),
      labelConjunction(labelRelTypeLeaf("A"), labelNegation(labelRelTypeLeaf("B"))),
      labelConjunction(labelOrRelTypeLeaf("A"), labelNegation(labelOrRelTypeLeaf("B")))
    )
  )

  private val variable = Seq(("", None), ("x", Some("x")))
  private val properties = Seq(("", None), ("{prop:1}", Some(mapOf(("prop", literalInt(1))))))
  private val pathLength = Seq(("", None), ("*1..5", Some(Some(range(Some(1), Some(5))))))
  private val where = Seq(("", None), ("WHERE x.prop = 1", Some(equals(prop("x", "prop"), literalInt(1)))))

  for {
    (expr, exprAstNode, exprAstRel, exprAstBoth) <- labelExpressions
  } yield {

    for {
      (maybeVariable, maybeVariableAst) <- variable
      (maybeProperties, maybePropertiesAst) <- properties
      (maybeWhere, maybeWhereAst) <- where
    } yield {

      // MATCH
      test(s"MATCH ($maybeVariable IS $expr $maybeProperties $maybeWhere)") {
        gives(
          singleQuery(
            match_(
              nodePat(
                maybeVariableAst,
                Some(exprAstNode),
                maybePropertiesAst,
                maybeWhereAst
              )
            )
          )
        )
      }

      // OPTIONAL MATCH
      test(s"OPTIONAL MATCH ($maybeVariable IS $expr $maybeProperties $maybeWhere)") {
        gives(
          singleQuery(
            optionalMatch(
              nodePat(
                maybeVariableAst,
                Some(exprAstNode),
                maybePropertiesAst,
                maybeWhereAst
              )
            )
          )
        )
      }

      for {
        (maybePathLength, maybePathLengthAst) <- pathLength
      } yield {
        // MATCH
        test(s"MATCH ()-[$maybeVariable IS $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
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
          s"OPTIONAL MATCH ()-[$maybeVariable IS $expr $maybePathLength $maybeProperties $maybeWhere]->()"
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
      }
    }

    // Inlined WHERE

    for {
      (maybeVariable, maybeVariableAst) <- variable
      (maybeProperties, maybePropertiesAst) <- properties
    } yield {
      test(s"MATCH ($maybeVariable $maybeProperties WHERE x IS $expr)") {
        gives(
          singleQuery(
            match_(
              nodePat(
                maybeVariableAst,
                None,
                maybePropertiesAst,
                Some(labelExpressionPredicate("x", exprAstBoth))
              )
            )
          )
        )
      }

      test(s"MATCH ($maybeVariable IS $expr $maybeProperties WHERE x IS $expr)") {
        gives(
          singleQuery(
            match_(
              nodePat(
                maybeVariableAst,
                Some(exprAstNode),
                maybePropertiesAst,
                Some(labelExpressionPredicate("x", exprAstBoth))
              )
            )
          )
        )
      }

      for {
        (maybePathLength, maybePathLengthAst) <- pathLength
      } yield {
        test(s"MATCH ()-[$maybeVariable $maybePathLength $maybeProperties WHERE x IS $expr]->()") {
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

        test(s"MATCH ()-[$maybeVariable IS $expr $maybePathLength $maybeProperties WHERE x IS $expr]->()") {
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
    }

    // WHERE

    test(s"MATCH (n) WHERE n IS $expr") {
      gives(
        singleQuery(
          match_(
            nodePat(Some("n")),
            Some(where(labelExpressionPredicate("n", exprAstBoth)))
          )
        )
      )
    }

    test(s"MATCH (n) WHERE n.prop = 1 AND n IS $expr") {
      gives(
        singleQuery(
          match_(
            nodePat(Some("n")),
            Some(where(
              and(
                equals(prop("n", "prop"), literalInt(1)),
                labelExpressionPredicate("n", exprAstBoth)
              )
            ))
          )
        )
      )
    }

    test(s"MATCH ()-[r]->()  WHERE r IS $expr") {
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

    test(s"MATCH ()-[r]->() WHERE r.prop = 1 AND r IS $expr") {
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

    test(s"MATCH (n) RETURN n IS $expr AS node") {
      gives(
        singleQuery(
          match_(
            nodePat(Some("n"))
          ),
          return_(
            aliasedReturnItem(
              labelExpressionPredicate("n", exprAstBoth),
              "node"
            )
          )
        )
      )
    }

    test(s"MATCH ()-[r]->() RETURN r IS $expr AS rel") {
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
                labelExpressionPredicate("n", labelConjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))),
                literalInt(1)
              ),
              (
                labelExpressionPredicate("r", labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))),
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

  test(s"MATCH (n IS IS)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            Some(labelLeaf("IS"))
          )
        )
      )
    )
  }

  test(s"MATCH (IS:IS)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("IS"),
            Some(labelLeaf("IS"))
          )
        )
      )
    )
  }

  test(s"MATCH (WHERE IS IS|WHERE WHERE WHERE.IS = IS.WHERE)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("WHERE"),
            labelExpression = Some(labelDisjunction(labelLeaf("IS"), labelLeaf("WHERE"))),
            predicates = Some(equals(prop("WHERE", "IS"), prop("IS", "WHERE")))
          )
        )
      )
    )
  }

  test(s"MATCH ()-[r IS IS]->()") {
    gives(
      singleQuery(
        match_(
          relationshipChain(
            nodePat(),
            relPat(
              Some("r"),
              Some(labelRelTypeLeaf("IS"))
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
              labelExpression = Some(labelDisjunction(labelRelTypeLeaf("IS"), labelRelTypeLeaf("WHERE"))),
              predicates = Some(equals(prop("WHERE", "IS"), prop("IS", "WHERE")))
            ),
            nodePat()
          )
        )
      )
    )
  }

  test(s"MATCH (n) WHERE n IS NOT AND n IS NOT NULL") {
    gives(
      singleQuery(
        match_(
          nodePat(Some("n")),
          where = Some(where(and(labelExpressionPredicate("n", labelOrRelTypeLeaf("NOT")), isNotNull(varFor("n")))))
        )
      )
    )
  }

  test(s"MATCH (n) WHERE n IS NULL AND n.prop IS NOT NULL") {
    gives(
      singleQuery(
        match_(
          nodePat(Some("n")),
          where = Some(where(and(isNull(varFor("n")), isNotNull(prop("n", "prop")))))
        )
      )
    )
  }

  test(s"MATCH (n WHERE n IS NULL)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            predicates = Some(isNull(varFor("n")))
          )
        )
      )
    )
  }

  test(s"MATCH (n IS NULL WHERE n IS NULL)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            labelExpression = Some(labelLeaf("NULL")),
            predicates = Some(isNull(varFor("n")))
          )
        )
      )
    )
  }

  test(s"MATCH (n WHERE n IS NOT NULL)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            predicates = Some(isNotNull(varFor("n")))
          )
        )
      )
    )
  }

  test(s"MATCH (n IS NOT NULL WHERE n IS NOT NULL)") {
    failsToParse
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
              labelExpression = Some(labelRelTypeLeaf("NULL")),
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
