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
    ("A", labelLeaf("A"), labelOrRelTypeLeaf("A")),
    (
      "A&B",
      labelConjunction(labelLeaf("A"), labelLeaf("B")),
      labelConjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))
    ),
    (
      "A|B",
      labelDisjunction(labelLeaf("A"), labelLeaf("B")),
      labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B"))
    ),
    ("%", labelWildcard(), labelWildcard()),
    ("!A", labelNegation(labelLeaf("A")), labelNegation(labelOrRelTypeLeaf("A"))),
    (
      "!(A|B)",
      labelNegation(labelDisjunction(labelLeaf("A"), labelLeaf("B"))),
      labelNegation(labelDisjunction(labelOrRelTypeLeaf("A"), labelOrRelTypeLeaf("B")))
    ),
    (
      "A&!B",
      labelConjunction(labelLeaf("A"), labelNegation(labelLeaf("B"))),
      labelConjunction(labelOrRelTypeLeaf("A"), labelNegation(labelOrRelTypeLeaf("B")))
    )
  )

  private val variable = Seq(("", None), ("x", Some("x")))
  private val properties = Seq(("", None), ("{prop:1}", Some(mapOf(("prop", literalInt(1))))))
  private val where = Seq(("", None), ("WHERE x.prop = 1", Some(equals(prop("x", "prop"), literalInt(1)))))

  for {
    (expr, exprAst, exprAstWhere) <- labelExpressions
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
                Some(exprAst),
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
                Some(exprAst),
                maybePropertiesAst,
                maybeWhereAst
              )
            )
          )
        )
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
                Some(labelExpressionPredicate("x", exprAstWhere))
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
                Some(exprAst),
                maybePropertiesAst,
                Some(labelExpressionPredicate("x", exprAstWhere))
              )
            )
          )
        )
      }
    }

    // WHERE

    test(s"MATCH (n) WHERE n IS $expr") {
      gives(
        singleQuery(
          match_(
            nodePat(Some("n")),
            Some(where(labelExpressionPredicate("n", exprAstWhere)))
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
                labelExpressionPredicate("n", exprAstWhere)
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
              labelExpressionPredicate("n", exprAstWhere),
              "node"
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
}
