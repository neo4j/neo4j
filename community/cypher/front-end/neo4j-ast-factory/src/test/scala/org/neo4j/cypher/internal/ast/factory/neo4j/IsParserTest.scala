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

import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Statement

class IsParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statements

  private val labelExpressions = createLabelExpression("IS", containsIs = true) ++
    createLabelExpression(":", containsIs = false)

  private def createLabelExpression(keyword: String, containsIs: Boolean) = Seq(
    (
      s"$keyword A",
      labelLeaf("A", containsIs = containsIs),
      labelRelTypeLeaf("A", containsIs = containsIs),
      labelOrRelTypeLeaf("A", containsIs = containsIs)
    ),
    (
      s"$keyword A&B",
      labelConjunction(
        labelLeaf("A", containsIs = containsIs),
        labelLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelOrRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword A|B",
      labelDisjunction(
        labelLeaf("A", containsIs = containsIs),
        labelLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelDisjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      ),
      labelDisjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelOrRelTypeLeaf("B", containsIs = containsIs),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword %",
      labelWildcard(containsIs = containsIs),
      labelWildcard(containsIs = containsIs),
      labelWildcard(containsIs = containsIs)
    ),
    (
      s"$keyword !A",
      labelNegation(labelLeaf("A", containsIs = containsIs), containsIs = containsIs),
      labelNegation(labelRelTypeLeaf("A", containsIs = containsIs), containsIs = containsIs),
      labelNegation(labelOrRelTypeLeaf("A", containsIs = containsIs), containsIs = containsIs)
    ),
    (
      s"$keyword !(A|B)",
      labelNegation(
        labelDisjunction(
          labelLeaf("A", containsIs = containsIs),
          labelLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      ),
      labelNegation(
        labelDisjunction(
          labelRelTypeLeaf("A", containsIs = containsIs),
          labelRelTypeLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      ),
      labelNegation(
        labelDisjunction(
          labelOrRelTypeLeaf("A", containsIs = containsIs),
          labelOrRelTypeLeaf("B", containsIs = containsIs),
          containsIs = containsIs
        ),
        containsIs = containsIs
      )
    ),
    (
      s"$keyword A&!B",
      labelConjunction(
        labelLeaf("A", containsIs = containsIs),
        labelNegation(labelLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelRelTypeLeaf("A", containsIs = containsIs),
        labelNegation(labelRelTypeLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
      ),
      labelConjunction(
        labelOrRelTypeLeaf("A", containsIs = containsIs),
        labelNegation(labelOrRelTypeLeaf("B", containsIs = containsIs), containsIs = containsIs),
        containsIs = containsIs
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
      test(s"MATCH ($maybeVariable $expr $maybeProperties $maybeWhere)") {
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
      test(s"OPTIONAL MATCH ($maybeVariable $expr $maybeProperties $maybeWhere)") {
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

      // EXISTS
      test(s"MATCH () WHERE EXISTS {($maybeVariable $expr $maybeProperties $maybeWhere)}") {

        val existsExpression: ExistsExpression = ExistsExpression(
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
        )(pos, None, None)

        gives(
          singleQuery(
            match_(
              pattern = nodePat(),
              Some(where(existsExpression))
            )
          )
        )
      }

      // COUNT
      test(s"MATCH () WHERE COUNT {($maybeVariable $expr $maybeProperties $maybeWhere)} = 1") {

        val countExpression: CountExpression = CountExpression(
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
        )(pos, None, None)

        gives(
          singleQuery(
            match_(
              pattern = nodePat(),
              Some(where(eq(countExpression, literalInt(1))))
            )
          )
        )
      }

      // CREATE + MERGE, these should parse but will be disallowed in semantic checking,
      // in a similar fashion as the label expressions

      test(s"CREATE ($maybeVariable $expr $maybeProperties $maybeWhere)") {
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

      test(s"MERGE ($maybeVariable $expr $maybeProperties $maybeWhere)") {
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
    }

    // Inlined WHERE

    for {
      (maybeVariable, maybeVariableAst) <- variable
      (maybeProperties, maybePropertiesAst) <- properties
    } yield {
      test(s"MATCH ($maybeVariable $maybeProperties WHERE x $expr)") {
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

      test(s"MATCH ($maybeVariable $expr $maybeProperties WHERE x $expr)") {
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
    }

    // WHERE

    test(s"MATCH (n) WHERE n $expr") {
      gives(
        singleQuery(
          match_(
            nodePat(Some("n")),
            Some(where(labelExpressionPredicate("n", exprAstBoth)))
          )
        )
      )
    }

    test(s"MATCH (n) WHERE n.prop = 1 AND n $expr") {
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

    test(s"MATCH (n) RETURN n $expr AS node") {
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

  test("MATCH (n) WHERE n IS :A|B RETURN n") {
    failsToParse
  }

  test("MATCH (n) WHERE n IS :A:B RETURN n") {
    failsToParse
  }

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

  test("WITH [1, 2, 3] AS where RETURN [is IN where] AS IS") {
    gives(
      singleQuery(
        with_(listOf(literalInt(1), literalInt(2), literalInt(3)).as("where")),
        return_(listComprehension(varFor("is"), varFor("where"), None, None).as("IS"))
      )
    )
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
