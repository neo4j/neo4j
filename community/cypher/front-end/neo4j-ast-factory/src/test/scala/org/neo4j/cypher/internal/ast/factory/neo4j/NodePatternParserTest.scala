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
import org.neo4j.cypher.internal.util.symbols.CTAny

class NodePatternParserTest extends PatternParserTestBase {

  test("MATCH ()") {
    parsesTo[Statements](
      singleQuery(
        match_(
          nodePat()
        )
      )
    )
  }

  test("MATCH (n)") {
    parsesTo[Statements](
      singleQuery(
        match_(
          nodePat(Some("n"))
        )
      )
    )
  }

  test("MATCH ({prop : 1})") {
    parsesTo[Statements](
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
    parsesTo[Statements](
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
    parsesTo[Statements](
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
        parsesTo[Statements](
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
        parsesTo[Statements](
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

        parsesTo[Statements](
          singleQuery(
            match_(
              pattern = nodePat(),
              where = Some(where(existsExpression))
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

        parsesTo[Statements](
          singleQuery(
            match_(
              pattern = nodePat(),
              where = Some(where(eq(countExpression, literalInt(1))))
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

        parsesTo[Statements](
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

      // CREATE + MERGE, these should parse, but all label expressions except : and & will be disallowed in semantic checking

      test(s"CREATE ($maybeVariable $expr $maybeProperties $maybeWhere)") {
        parsesTo[Statements](
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
        parsesTo[Statements](
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
        parsesTo[Statements](
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
        parsesTo[Statements](
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
      parsesTo[Statements](
        singleQuery(
          match_(
            nodePat(Some("n")),
            where = Some(where(labelExpressionPredicate("n", exprAstBoth)))
          )
        )
      )
    }

    test(s"MATCH (n) WHERE n.prop = 1 AND n $expr") {
      parsesTo[Statements](
        singleQuery(
          match_(
            nodePat(Some("n")),
            where = Some(where(
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
      parsesTo[Statements](
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
    parsesTo[Statements](
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
    failsToParse[Statements]()
  }

  test("MATCH (n) WHERE n IS :A:B RETURN n") {
    failsToParse[Statements]()
  }

  test(s"MATCH (n IS IS)") {
    parsesTo[Statements](
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
    parsesTo[Statements](
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
    parsesTo[Statements](
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
    failsToParse[Statements]()
  }

  test(s"MATCH (n) WHERE n IS NULL AND n.prop IS NOT NULL") {
    parsesTo[Statements](
      singleQuery(
        match_(
          nodePat(Some("n")),
          where = Some(where(and(isNull(varFor("n")), isNotNull(prop("n", "prop")))))
        )
      )
    )
  }

  test(s"MATCH (n WHERE n IS NULL)") {
    parsesTo[Statements](
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

  test(s"MATCH (n: NULL WHERE n IS NULL)") {
    parsesTo[Statements](
      singleQuery(
        match_(
          nodePat(
            Some("n"),
            labelExpression = Some(labelLeaf("NULL", containsIs = false)),
            predicates = Some(isNull(varFor("n")))
          )
        )
      )
    )
  }

  test(s"MATCH (n WHERE n IS NOT NULL)") {
    parsesTo[Statements](
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
    failsToParse[Statements]()
  }

  // Labels NOT, NULL and TYPED are not allowed together with IS keyword unless escaped
  for {
    label <- Seq("NOT", "NULL", "TYPED")
  } yield {
    test(s"MATCH (n:$label)") {
      parsesTo[Statements](
        singleQuery(
          match_(
            nodePat(
              Some("n"),
              Some(labelLeaf(label))
            )
          )
        )
      )
    }

    test(s"MATCH (n:`$label`)") {
      parsesTo[Statements](
        singleQuery(
          match_(
            nodePat(
              Some("n"),
              Some(labelLeaf(label))
            )
          )
        )
      )
    }

    test(s"MATCH (n IS $label)") {
      failsToParse[Statements]()
    }

    test(s"MATCH (n IS `$label`)") {
      parsesTo[Statements](
        singleQuery(
          match_(
            nodePat(
              Some("n"),
              Some(labelLeaf(label, containsIs = true))
            )
          )
        )
      )
    }
  }

  test("WITH [1, 2, 3] AS where RETURN [is IN where] AS IS") {
    parsesTo[Statements](
      singleQuery(
        with_(listOf(literalInt(1), literalInt(2), literalInt(3)).as("where")),
        return_(listComprehension(varFor("is"), varFor("where"), None, None).as("IS"))
      )
    )
  }
}
