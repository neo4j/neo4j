/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ast.semantics.SemanticTable.TypeGetter
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LimitBeforeCountRewriter.limitSafeExpressionFrom
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection.OptionalPreprocessing
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection.OptionalPreprocessing.FilterAndLimit
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection.OptionalPreprocessing.Passthrough
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

class LimitBeforeCountRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  private val intParam = "$intParam"
  private val stringParam = "$stringParam"
  private val intListParam = "$intListParam"
  private val lowerBoundParam = "$lowerBoundParam"
  private val upperBoundParam = "$upperBoundParam"

  private val paramTypes: Map[String, CypherType] = Map(
    intParam -> CTInteger,
    stringParam -> CTString,
    intListParam -> CTList(CTInteger),
    lowerBoundParam -> CTInteger,
    upperBoundParam -> CTInteger
  )

  private val supportedOperators: Seq[String] = Seq(">", ">=", "<", "<=", "=", "<>")

  private val unsupportedOperators: Seq[String] = Seq(
    s"STARTS WITH $stringParam",
    s"ENDS WITH $stringParam",
    s"CONTAINS $stringParam",
    s"IN $intListParam",
    "IS NULL",
    "IS NOT NULL"
  )

  private def param(paramNameWithDollarPrefix: String): Parameter =
    parameter(paramNameWithDollarPrefix.substring(1), CTAny)

  test("rewrites COUNT {} with supported operators and int literal") {
    val lit = 123
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(s"RETURN COUNT { (n)-[r]->(n) } $op $lit AS result") {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_0" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(literal(lit)))
            )
          )
      }
    }
  }

  test("rewrites COUNT {} with supported operators and int parameter in RETURN") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(s"RETURN COUNT { (n)-[r]->(n) } $op $intParam AS result") {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_0" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam)))
            )
          )
      }
    }
  }

  test("rewrites COUNT {} supported operators with int parameter in WHERE") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(s"MATCH (n) WHERE COUNT { (n)-[r]->(n) } $op $intParam RETURN n AS result") {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_0" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
              importedExposedSymbols = Set(v"n")
            )
          )
      }
    }
  }

  test("does not rewrite COUNT {} with supported operators and string literal") {
    val lit = "'hello'"
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"RETURN COUNT { (n)-[r]->(n) } $op $lit AS result")
    }
  }

  test("does not rewrite COUNT {} with supported operators and string parameter") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"RETURN COUNT { (n)-[r]->(n) } $op $stringParam AS result")
    }
  }

  test("does not rewrite COUNT {} with explicit LIMIT ") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"RETURN COUNT { MATCH (n)-[r]->(n) LIMIT 100 } $op $intParam AS result")
    }
  }

  test("does not rewrite COUNT {} with unsupported operators") {
    for (op <- unsupportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"RETURN COUNT { (n)-[r]->(n) } $op AS result")
    }
  }

  test("does not rewrite COUNT {} with supported operators and non-const expression") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"MATCH (n) WHERE COUNT { (n)-[r]->(n) } $op n.prop RETURN n")
    }
  }

  test("rewrites COUNT {... UNION ...} with supported operators") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      val query =
        s"""RETURN COUNT {
           |  MATCH (n)-[r]->(n)
           |  RETURN n
           | UNION
           |  MATCH (n)-[r]->(n)
           |  RETURN n
           |} $op $intParam AS result""".stripMargin

      assertRewrite(query) {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_5" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam)))
            )
          )
      }
    }
  }

  test("rewrites ANDed COUNT {}") {
    for (Seq(op1, op2) <- supportedOperators.sliding(2, 1)) withClue(s"operators: $op1, $op2\n") {
      assertRewrite(
        s"""MATCH (n)
           |WHERE
           |  COUNT { (n)-[r:REL]->(n) } $op1 $intParam
           |  AND COUNT { (n)-[r:OTHER_REL]->(n) } $op2 $intParam
           |RETURN n AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_2" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
            importedExposedSymbols = Set(v"n")
          ),
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_3" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
            importedExposedSymbols = Set(v"n")
          )
        )
      }
    }
  }

  test("rewrites ORed COUNT {}") {
    for (Seq(op1, op2) <- supportedOperators.sliding(2, 1)) withClue(s"operators: $op1, $op2\n") {
      assertRewrite(
        s"""MATCH (n)
           |WHERE
           |  COUNT { (n)-[r:REL]->(n) } $op1 $intParam
           |  OR COUNT { (n)-[r:OTHER_REL]->(n) } $op2 $intParam
           |RETURN n AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_2" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
            importedExposedSymbols = Set(v"n")
          ),
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_3" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
            importedExposedSymbols = Set(v"n")
          )
        )
      }
    }
  }

  test("rewrites COUNT {} with chained supported operators") {
    for (op <- supportedOperators) withClue(s"operators: $op\n") {
      assertRewrite(
        s"""MATCH (n)
           |WHERE
           |  $lowerBoundParam $op COUNT { (n)-[r]->(n) } $op $upperBoundParam
           |RETURN n AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations should contain theSameElementsAs Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_2" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(lowerBoundParam))),
            importedExposedSymbols = Set(v"n")
          ),
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_3" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(upperBoundParam))),
            importedExposedSymbols = Set(v"n")
          )
        )
      }
    }
  }

  test("rewrites nested COUNT {}") {
    for (op <- supportedOperators) withClue(s"operators: $op\n") {
      assertRewrite(
        s"""MATCH (n)
           |WHERE
           |  COUNT {
           |    MATCH (n)-[r]->(m)
           |    WHERE COUNT { (m)-[rr]->(m) } $op $lowerBoundParam
           |  } $op $upperBoundParam
           |RETURN n AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(upperBoundParam))),
            importedExposedSymbols = Set(v"n")
          ),
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_1" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(lowerBoundParam))),
            importedExposedSymbols = Set(v"m")
          )
        )
      }
    }
  }

  test("rewrites count(*) with supported operators and int literal") {
    val lit = 123
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(s"MATCH (n)-[r]->(n) RETURN count(*) $op $lit AS result") {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_0" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(literal(lit)))
            )
          )
      }
    }
  }

  test("rewrites count(*) with supported operators and int param") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(s"MATCH (n)-[r]->(n) RETURN count(*) $op $intParam AS result") {
        rewrittenAggregations =>
          rewrittenAggregations shouldEqual Seq(
            AggregatingQueryProjection(
              aggregationExpressions = Map(v"anon_0" -> countStar()),
              optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam)))
            )
          )
      }
    }
  }

  test("rewrites count(*) in CALL") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(
        s"""MATCH (n)
           |CALL (n) {
           |  MATCH (n)-[r]->(n)
           |  RETURN count(*) $op $intParam AS result
           |}
           |RETURN n, result""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> countStar()),
            optionalPreprocessing = FilterAndLimit(None, limitSafeExpressionFrom(param(intParam))),
            importedExposedSymbols = Set(v"n")
          )
        )
      }
    }
  }

  test("does not rewrite count(*) with grouping columns") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(
        s"MATCH (n)-[r]->(n) RETURN n, count(*) $op $intParam AS result"
      )
    }
  }

  test("does not rewrites count(*) if the full count is used") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(
        s"""MATCH (n)-[r]->(n)
           |WITH count(*) AS c
           |RETURN c, c $op $intParam AS result
           |""".stripMargin
      )
    }
  }

  test("does not rewrite count(*) with unsupported operators") {
    for (op <- unsupportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"MATCH (n)-[r]->(n) RETURN count(*) $op AS result")
    }
  }

  test("does not rewrite count(*) with explicit LIMIT ") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"MATCH (n)-[r]->(n) LIMIT 100 RETURN count(*) $op $intParam AS result")
    }
  }

  test("rewrites count(n.prop) with supported operators and int literal") {
    val lit = 123
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(
        s"""MATCH (n)-[r]->(n)
           |RETURN count(n.prop) $op $lit AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> count(prop("n", "prop"))),
            optionalPreprocessing = FilterAndLimit(
              Some(isNotNull(prop("n", "prop"))),
              limitSafeExpressionFrom(literal(lit))
            )
          )
        )
      }
    }
  }

  test("rewrites count(n.prop) with supported operators and int param") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(
        s"""MATCH (n)-[r]->(n)
           |RETURN count(n.prop) $op $intParam AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> count(prop("n", "prop"))),
            optionalPreprocessing = FilterAndLimit(
              Some(isNotNull(prop("n", "prop"))),
              limitSafeExpressionFrom(param(intParam))
            )
          )
        )
      }
    }
  }

  test("rewrites count(n) with supported operators and int param") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(
        s"""MATCH (n)-[r]->(n)
           |RETURN count(n) $op $intParam AS result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> count(v"n")),
            optionalPreprocessing = FilterAndLimit(
              Some(isNotNull(v"n")),
              limitSafeExpressionFrom(param(intParam))
            )
          )
        )
      }
    }
  }

  test("rewrites count(n.prop) IN singleton list") {
    assertRewrite(
      s"""MATCH (n)
         |RETURN count(n.prop) IN [$intParam] AS result
         |""".stripMargin
    ) { rewrittenAggregations =>
      rewrittenAggregations shouldEqual Seq(
        AggregatingQueryProjection(
          aggregationExpressions = Map(v"anon_0" -> count(prop("n", "prop"))),
          optionalPreprocessing = FilterAndLimit(
            Some(isNotNull(prop("n", "prop"))),
            limitSafeExpressionFrom(param(intParam))
          )
        )
      )
    }
  }

  test("rewrites count(n.prop) in CALL") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertRewrite(
        s"""MATCH (n)
           |CALL (n) {
           |  MATCH (n)-[r]->(n)
           |  RETURN count(n.prop) $op $intParam AS result
           |}
           |RETURN n, result
           |""".stripMargin
      ) { rewrittenAggregations =>
        rewrittenAggregations shouldEqual Seq(
          AggregatingQueryProjection(
            aggregationExpressions = Map(v"anon_0" -> count(prop("n", "prop"))),
            optionalPreprocessing = FilterAndLimit(
              Some(isNotNull(prop("n", "prop"))),
              limitSafeExpressionFrom(param(intParam))
            ),
            importedExposedSymbols = Set(v"n")
          )
        )
      }
    }
  }

  test("does not rewrite count(DISTINCT n.prop)") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(
        s"""MATCH (n)-[r]->(n)
           |RETURN count(DISTINCT n.prop) $op $intParam AS result
           |""".stripMargin
      )
    }
  }

  test("does not rewrite count(n.prop) if the full count is used") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(
        s"""MATCH (n)-[r]->(n)
           |WITH count(n.prop) AS c
           |RETURN c, c $op $intParam AS result
           |""".stripMargin
      )
    }
  }

  test("does not rewrite count(n.prop) with unsupported operators") {
    for (op <- unsupportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"MATCH (n)-[r]->(n) RETURN count(n.prop) $op AS result")
    }
  }

  test("does not rewrite count(...) with complex expression") {
    val expressions = Seq(
      "n.prop + 1",
      "EXISTS { () }",
      "n IS NOT NULL",
      "CASE WHEN n.prop = 1 THEN 1 ELSE null END"
    )
    for {
      op <- supportedOperators
      expr <- expressions
    } withClue(s"operator: $op, expression: $expr\n") {
      assertNoRewrite(
        s"""MATCH (n)-[r]->(n)
           |RETURN count($expr) $op $intParam AS result
           |""".stripMargin
      )
    }
  }

  test("does not rewrite count(n.prop) with explicit LIMIT ") {
    for (op <- supportedOperators) withClue(s"operator: $op\n") {
      assertNoRewrite(s"MATCH (n)-[r]->(n) LIMIT 100 RETURN count(n.prop) $op $intParam AS result")
    }
  }

  private val setOptionalPreprocessingToPassthrough: Rewriter =
    bottomUp { Rewriter.lift { case _: OptionalPreprocessing => Passthrough } }

  private def assertRewrite(
    query: String
  )(
    assertRewrittenAggregations: Seq[AggregatingQueryProjection] => Assertion
  ): Unit = {
    val semanticTable = mock[SemanticTable]
    when(semanticTable.typeFor(any[Expression])).thenAnswer {
      (invocation: InvocationOnMock) =>
        val tpe = invocation.getArguments.head match {
          case _: IntegerLiteral => Some(CTInteger)
          case p: Parameter      => paramTypes.get("$" + p.name).orElse(Some(p.parameterType))
          case _                 => None
        }
        TypeGetter(tpe.map(_.invariant))
    }

    val logicalPlanState = mock[LogicalPlanState]
    when(logicalPlanState.semanticTable()).thenReturn(semanticTable)

    val cypherPlannerConfiguration = mock[CypherPlannerConfiguration]
    when(cypherPlannerConfiguration.limitBeforeCountRewriterEnabled).thenReturn(() => true)

    val plannerContext = mock[PlannerContext]
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    when(plannerContext.config).thenReturn(cypherPlannerConfiguration)

    val rewriter = LimitBeforeCountRewriter.instance(logicalPlanState, plannerContext)

    val plannerQueryBeforeRewrite = buildPlannerQuery(query)

    val rewrittenPlannerQuery = plannerQueryBeforeRewrite.endoRewrite(rewriter)

    withClue(
      s"rewriter should only introduce OptionalPreprocessing, and not change anything else\n\n"
    ) {
      val plannerQueryWithRewriteUndone = rewrittenPlannerQuery.endoRewrite(setOptionalPreprocessingToPassthrough)
      try {
        plannerQueryBeforeRewrite shouldEqual plannerQueryWithRewriteUndone
      } catch {
        case e: TestFailedException =>
          plannerQueryBeforeRewrite compareAsPrettyStrings plannerQueryWithRewriteUndone
          throw e
      }
    }

    withClue(s"should rewrite everything in one pass\n\n") {
      val twiceRewritten = rewrittenPlannerQuery.endoRewrite(rewriter)
      try {
        rewrittenPlannerQuery shouldEqual twiceRewritten
      } catch {
        case e: TestFailedException =>
          rewrittenPlannerQuery compareAsPrettyStrings twiceRewritten
          throw e
      }
    }

    assertRewrittenAggregations(
      rewrittenPlannerQuery
        .folder
        .findAllByClass[AggregatingQueryProjection]
        .sortBy(_.aggregationExpressions.head._2.position)
    )
  }

  private def assertNoRewrite(query: String): Unit = {
    assertRewrite(query) { rewrittenAggregations =>
      rewrittenAggregations.map(_.optionalPreprocessing) shouldEqual
        Seq.fill(rewrittenAggregations.size)(Passthrough)
    }
  }
}
