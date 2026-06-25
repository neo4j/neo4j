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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Normalizes the WHERE predicate of a SEARCH query in order to simplify validation, semantic checking and planning.
 *
 * For example queries like:
 * {{{
 *   MATCH (n)
 *     SEARCH n IN (
 *       VECTOR INDEX indexName
 *       FOR $embedding
 *       WHERE [filter]
 *       LIMIT 10
 *     )
 * }}}
 *
 * The normalization does the following:
 *
 *  - Puts the property expression on the LHS of binary expressions, i.e. `5 < n.p` -> `n.p < 5`
 *  - Rewrites `WHERE n.bool` to `WHERE n.bool = TRUE`
 *  - Rewrites `WHERE NOT n.bool` to `WHERE n.bool = FALSE`
 */
case object SearchPredicateNormalizer extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter = instance

  val instance: Rewriter = bottomUp {
    Rewriter.lift {
      case search @ Search(bindingVariable, _, _, _, Some(filter), _) =>
        val rawBooleanPropertyPredicates = findBooleanPropertyPredicates(filter.expression, bindingVariable)
        val rewriter = filterRewriter(bindingVariable, rawBooleanPropertyPredicates)
        val newFilter = filter.endoRewrite(rewriter)
        search.copy(where = Some(newFilter))(search.position)
    }
  }

  private def filterRewriter(
    bindingVariable: LogicalVariable,
    rawPropertiesWithReplacement: Map[Expression, Expression]
  ) = {
    bottomUp {
      Rewriter.lift {
        case ie @ InequalityExpression(Property(v1, _), Property(v2, _))
          if v1 == bindingVariable && v2 == bindingVariable => ie
        case ie @ InequalityExpression(_, Property(variable, _)) if variable == bindingVariable => ie.swapped
        case e @ Equals(Property(v1, _), Property(v2, _)) if v1 == bindingVariable && v2 == bindingVariable => e
        case e @ Equals(_, Property(variable, _)) if variable == bindingVariable => e.switchSides
        case p: Expression if rawPropertiesWithReplacement.contains(p)           => rawPropertiesWithReplacement(p)
      }
    }
  }

  private def findBooleanPropertyPredicates(expression: Expression, bindingVariable: LogicalVariable) = {
    def replaceProperty(p: Property, isInverted: Boolean) =
      if (isInverted) {
        Equals(p, False()(p.position.zeroLength))(p.position)
      } else {
        Equals(p, True()(p.position.zeroLength))(p.position)
      }

    expression.folder.treeFold(IdentityMap.empty[Expression, Expression]) {
      case n: Not =>
        var isInverted = false
        var e: Expression = n
        while (e.isInstanceOf[Not]) {
          e match {
            case not: Not =>
              e = not.rhs
              isInverted = !isInverted
            case _ =>
          }
        }
        e match {
          case p: Property if p.map == bindingVariable =>
            acc => SkipChildren(acc + (n, replaceProperty(p, isInverted)))
          case _ => acc => SkipChildren(acc)
        }
      case p: Property if p.map == bindingVariable =>
        acc => SkipChildren(acc + (p, replaceProperty(p, isInverted = false)))
      case _: And               => acc => TraverseChildren(acc)
      case _: BooleanExpression => acc => SkipChildren(acc)
    }
  }
}
