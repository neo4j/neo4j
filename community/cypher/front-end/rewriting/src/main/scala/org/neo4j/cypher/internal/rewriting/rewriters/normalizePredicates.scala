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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object normalizePredicates extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // unnamed pattern cannot be rewritten, so they need to be handled first
    noUnnamedNodesAndRelationships,
    // Pattern comprehensions must have been rewritten to COLLECT
    containsNoNodesOfType[PatternComprehension](),
    // Pattern expressions must have been rewritten to EXISTS
    containsNoNodesOfType[PatternExpression]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable + AndRewrittenToAnds

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
    case m @ Match(_, _, pattern, _, where) =>
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

    case part @ PatternPartWithSelector(_: SelectiveSelector, _) =>
      part.element match {
        case path: ParenthesizedPath =>
          val predicates = normalizer.extractAllFrom(path.part.element) ++ path.optionalWhereClause
          val newPredicate = predicates.reduceOption(And(_, _)(path.position))
          val newElement = ParenthesizedPath(
            part = normalizer.replaceAllIn(path.part),
            optionalWhereClause = newPredicate
          )(path.position)
          part.replaceElement(newElement)
        case otherElement =>
          normalizer.extractAllFrom(otherElement).reduceOption(And(_, _)(part.position)) match {
            // We should not wrap the pattern in new parentheses if there is no predicate to add
            case None => part
            case Some(newPredicate) =>
              val syntheticPatternPart =
                PathPatternPart(normalizer.replaceAllIn(otherElement))
              val newElement = ParenthesizedPath(syntheticPatternPart, Some(newPredicate))(part.position)
              part.replaceElement(newElement)
          }
      }

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
