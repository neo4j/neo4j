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
      labelLeaf("A", containsIs = true),
      labelRelTypeLeaf("A", containsIs = true),
      labelOrRelTypeLeaf("A", containsIs = true)
    ),
    (
      "A&B",
      labelConjunction(labelLeaf("A", containsIs = true), labelLeaf("B", containsIs = true), containsIs = true),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = true),
        labelRelTypeLeaf("B", containsIs = true),
        containsIs = true
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = true),
        labelOrRelTypeLeaf("B", containsIs = true),
        containsIs = true
      )
    ),
    (
      "A|B",
      labelDisjunction(labelLeaf("A", containsIs = true), labelLeaf("B", containsIs = true), containsIs = true),
      labelDisjunction(
        labelRelTypeLeaf("A", containsIs = true),
        labelRelTypeLeaf("B", containsIs = true),
        containsIs = true
      ),
      labelDisjunction(
        labelOrRelTypeLeaf("A", containsIs = true),
        labelOrRelTypeLeaf("B", containsIs = true),
        containsIs = true
      )
    ),
    (
      "%",
      labelWildcard(containsIs = true),
      labelWildcard(containsIs = true),
      labelWildcard(containsIs = true)
    ),
    (
      "!A",
      labelNegation(labelLeaf("A", containsIs = true), containsIs = true),
      labelNegation(labelRelTypeLeaf("A", containsIs = true), containsIs = true),
      labelNegation(labelOrRelTypeLeaf("A", containsIs = true), containsIs = true)
    ),
    (
      "!(A|B)",
      labelNegation(
        labelDisjunction(labelLeaf("A", containsIs = true), labelLeaf("B", containsIs = true), containsIs = true),
        containsIs = true
      ),
      labelNegation(
        labelDisjunction(
          labelRelTypeLeaf("A", containsIs = true),
          labelRelTypeLeaf("B", containsIs = true),
          containsIs = true
        ),
        containsIs = true
      ),
      labelNegation(
        labelDisjunction(
          labelOrRelTypeLeaf("A", containsIs = true),
          labelOrRelTypeLeaf("B", containsIs = true),
          containsIs = true
        ),
        containsIs = true
      )
    ),
    (
      "A&!B",
      labelConjunction(
        labelLeaf("A", containsIs = true),
        labelNegation(labelLeaf("B", containsIs = true), containsIs = true),
        containsIs = true
      ),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = true),
        labelNegation(labelRelTypeLeaf("B", containsIs = true), containsIs = true),
        containsIs = true
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = true),
        labelNegation(labelOrRelTypeLeaf("B", containsIs = true), containsIs = true),
        containsIs = true
      )
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

      // CREATE + MERGE, these should parse but will be disallowed in semantic checking,
      // in a similar fashion as the label expressions

      test(s"CREATE ($maybeVariable IS $expr $maybeProperties $maybeWhere)") {
        gives(
          singleQuery(
            create(
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

      test(s"MERGE ($maybeVariable IS $expr $maybeProperties $maybeWhere)") {
        gives(
          singleQuery(
            merge(
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

        // CREATE + MERGE, these should parse but will be disallowed in semantic checking,
        // in a similar fashion as the label expressions

        test(s"CREATE ()-[$maybeVariable IS $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
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

        test(s"MERGE ()-[$maybeVariable IS $expr $maybePathLength $maybeProperties $maybeWhere]->()") {
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

  test(s"MATCH (n IS IS)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            Some(labelLeaf("IS", containsIs = true))
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
            labelExpression = Some(labelDisjunction(
              labelLeaf("IS", containsIs = true),
              labelLeaf("WHERE", containsIs = true),
              containsIs = true
            )),
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

  test(s"MATCH (n) WHERE n IS NOT AND n IS NOT NULL") {
    gives(
      singleQuery(
        match_(
          nodePat(Some("n")),
          where = Some(where(and(
            labelExpressionPredicate("n", labelOrRelTypeLeaf("NOT", containsIs = true)),
            isNotNull(varFor("n"))
          )))
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
            labelExpression = Some(labelLeaf("NULL", containsIs = true)),
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

  test("Should not allow IS for SET label") {
    val query =
      """MATCH (n)
        |SET n IS Label
        |RETURN n""".stripMargin

    assertFailsWithMessage(query, "Invalid input 'IS': expected \":\" (line 2, column 7 (offset: 16))")
  }

  test("Should not allow IS for REMOVE label") {
    val query =
      """MATCH (n)
        |REMOVE n IS Label
        |RETURN n""".stripMargin

    assertFailsWithMessage(query, "Invalid input 'IS': expected \":\" (line 2, column 10 (offset: 19))")
  }
}
