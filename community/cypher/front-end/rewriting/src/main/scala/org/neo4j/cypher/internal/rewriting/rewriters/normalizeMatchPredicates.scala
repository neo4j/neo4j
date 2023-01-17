/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedPatternElementsInMatch
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.topDown

case object NoPredicatesInNamedPartsOfMatchPattern extends StepSequencer.Condition

object normalizeMatchPredicates extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedPatternElementsInMatch // unnamed pattern cannot be rewritten, so they need to handled first
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(NoPredicatesInNamedPartsOfMatchPattern)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getRewriter(semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory,
                           anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = {
    normalizeMatchPredicates(normalizeInlinedWhereClauses) andThen
    normalizeMatchPredicatesInExists(normalizeInlinedWhereClauses) andThen
    normalizeMatchPredicates(normalizeLabelAndPropertyPredicates(anonymousVariableNameGenerator))
  }

  /**
   * Normalizer that moves inlined node WHERE predicates to the outside WHERE clause.
   * This needs to be the first predicate normalizer that runs, because of scoping.
   * For example, when a node pattern has an inlined node predicate that introduces new variables, e.g. `(n WHERE EXISTS {(:A)})`, we need to avoid extracting
   * the `:A` predicate to the outermost scope.
   *
   * @note Needs to run before [[normalizeLabelAndPropertyPredicates]].
   */
  def normalizeInlinedWhereClauses: MatchPredicateNormalizer =
    MatchPredicateNormalizerChain(
      NodePatternPredicateNormalizer
    )

   /**
   * Normalizer that moves inlined node property predicates and label expression predicates to the outside WHERE clause.
   *
   * @note [[normalizeInlinedWhereClauses]] needs to run before this normalizer, to correctly handle scoping of nested WHERE clauses.
   */
  def normalizeLabelAndPropertyPredicates(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): MatchPredicateNormalizer =
    MatchPredicateNormalizerChain(
      PropertyPredicateNormalizer(anonymousVariableNameGenerator),
      LabelPredicateNormalizer
    )
}

case class normalizeMatchPredicates(normalizer: MatchPredicateNormalizer) extends Rewriter {
  override def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m@Match(_, pattern, _, where) =>
      val predicates = normalizer.extractAllFrom(pattern)
      val rewrittenPredicates = predicates ++ where.map(_.expression)
      val predOpt: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(m.position))

      val newWhere: Option[Where] = predOpt.map {
        exp =>
          val pos: InputPosition = where.fold(m.position)(_.position)
          Where(exp)(pos)
      }

      m.copy(
        pattern = pattern.endoRewrite(topDown(Rewriter.lift(normalizer.replace))),
        where = newWhere
      )(m.position)
  }

  private val instance = topDown(rewriter, _.isInstanceOf[Expression])
}

// Normalize pattern predicates inside EXISTS {} to make sure that later rewriters handle them correctly
case class normalizeMatchPredicatesInExists(normalizer: MatchPredicateNormalizer) extends Rewriter {
  override def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case w@Where(expression) =>
      val newExpression = expression.endoRewrite(topDown(Rewriter.lift {
        case e@ExistsSubClause(pattern, optionalWhereExpression) =>
          val predicates = normalizer.extractAllFrom(pattern)
          val rewrittenPredicates = predicates ++ optionalWhereExpression
          val newOptionalWhereExpression: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(e.position))

          e.copy(
            pattern = pattern.endoRewrite(topDown(Rewriter.lift(normalizer.replace))),
            optionalWhereExpression = newOptionalWhereExpression
          )(e.position, e.outerScope)
      }))

      w.copy(newExpression)(w.position)
  }


  private val instance = topDown(rewriter, _.isInstanceOf[Expression])
}
