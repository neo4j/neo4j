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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object NoNodeOrRelationshipPredicates extends StepSequencer.Condition

case object normalizePredicates extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedNodesAndRelationships // unnamed pattern cannot be rewritten, so they need to be handled first
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(NoNodeOrRelationshipPredicates)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = normalizePredicates(PredicateNormalizer.normalizeInlinedWhereClauses) andThen
    normalizePredicates(PredicateNormalizer.normalizeLabelAndPropertyPredicates(anonymousVariableNameGenerator))
}

case class normalizePredicates(normalizer: PredicateNormalizer) extends Rewriter {
  override def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m @ Match(_, pattern, _, where) =>
      val predicates = normalizer.extractAllFrom(pattern)
      val rewrittenPredicates = predicates ++ where.map(_.expression)
      val predOpt: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(m.position))

      val newWhere: Option[Where] = predOpt.map {
        exp =>
          val pos: InputPosition = where.fold(m.position)(_.position)
          Where(exp)(pos)
      }

      m.copy(
        pattern = normalizer.replaceAllIn(pattern),
        where = newWhere
      )(m.position)

    case p: PatternComprehension =>
      val predicates = normalizer.extractAllFrom(p.pattern)
      val rewrittenPredicates = predicates ++ p.predicate
      val newPredicate: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(p.position))

      p.copy(
        pattern = normalizer.replaceAllIn(p.pattern),
        predicate = newPredicate
      )(p.position, p.computedIntroducedVariables, p.computedScopeDependencies)

    case qp @ QuantifiedPath(patternPart, _, optionalWhereExpression, _) =>
      val predicates = normalizer.extractAllFrom(patternPart)
      val rewrittenPredicates = predicates ++ optionalWhereExpression
      val newOptionalWhereExpression: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(qp.position))

      qp.copy(
        part = normalizer.replaceAllIn(patternPart),
        optionalWhereExpression = newOptionalWhereExpression
      )(qp.position)
  }

  private val instance = topDown(rewriter)
}
