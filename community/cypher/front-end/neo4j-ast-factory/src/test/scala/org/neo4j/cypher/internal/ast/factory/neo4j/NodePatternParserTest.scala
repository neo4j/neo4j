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
import org.neo4j.cypher.internal.util.symbols.CTAny

class NodePatternParserTest extends PatternParserTestBase {

  test("MATCH ()") {
    gives(
      singleQuery(
        match_(
          nodePat()
        )
      )
    )
  }

  test("MATCH (n)") {
    gives(
      singleQuery(
        match_(
          nodePat(Some("n"))
        )
      )
    )
  }

  test("MATCH ({prop : 1})") {
    gives(
      singleQuery(
        match_(
          nodePat(
            properties = Some(mapOf(("prop", literalInt(1))))
          )
        )
      )
    )
  }

  test("MATCH ($props)") {
    gives(
      singleQuery(
        match_(
          nodePat(
            properties = Some(ExplicitParameter("props", CTAny)(pos))
          )
        )
      )
    )
  }

  test("MATCH (n {prop: 1})") {
    gives(
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            properties = Some(mapOf(("prop", literalInt(1))))
          )
        )
      )
    )
  }

  for {
    (expr, exprAstNode, _, exprAstBoth) <- labelExpressions
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

      // COLLECT
      test(
        s"""
           |MATCH ()
           |RETURN COLLECT {
           |  MATCH ($maybeVariable $expr $maybeProperties $maybeWhere)
           |  RETURN 42 AS answer
           |} AS collect
           |""".stripMargin
      ) {

        val collectExpression: CollectExpression = CollectExpression(
          singleQuery(
            match_(
              nodePat(
                maybeVariableAst,
                Some(exprAstNode),
                maybePropertiesAst,
                maybeWhereAst
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
              pattern = nodePat()
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
                labelExpressionPredicate(
                  "n",
                  labelConjunction(
                    labelOrRelTypeLeaf("A", containsIs = true),
                    labelOrRelTypeLeaf("B", containsIs = true),
                    containsIs = true
                  )
                ),
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
