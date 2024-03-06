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

import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.conditions.PredicatesSimplified
import org.neo4j.cypher.internal.rewriting.conditions.SizeOfCollectRewrittenToCount
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Adds an EXISTS { ... } around any pattern expression that is expected to produce a boolean e.g.
 *
 * MATCH (n) WHERE (n)-->(m) RETURN n
 *
 * is rewritten to
 *
 * MATCH (n) WHERE EXISTS { (n)-->(m) } RETURN n
 *
 * Any Exists FunctionInvocation is also rewritten to [[ExistsExpression]].
 *
 * Rewrite equivalent expressions with `size` or `length` to `exists`.
 * This rewrite normalizes this cases and make it easier to plan correctly.
 *
 * [[simplifyPredicates]] takes care of rewriting the Not(Not(Exists(...))) which can be introduced by this rewriter.
 *
 * This rewriter needs to run before [[nameAllPatternElements]], which rewrites pattern expressions. Otherwise we don't find them in the semantic table.
 */
case class normalizeExistsPatternExpressions(
  semanticState: SemanticState
) extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case p: PatternExpression if semanticState.expressionType(p).expected.contains(symbols.CTBoolean.invariant) =>
      ExistsExpression(PatternToQueryConverter.convertPatternToQuery(
        p.pattern.element,
        None,
        p.position
      ))(
        p.position,
        p.computedIntroducedVariables,
        p.computedScopeDependencies
      )
    case Exists(p: PatternExpression) =>
      ExistsExpression(PatternToQueryConverter.convertPatternToQuery(
        p.pattern.element,
        None,
        p.position
      ))(
        p.position,
        p.computedIntroducedVariables,
        p.computedScopeDependencies
      )

    case GreaterThan(CountLikeToExistsConverter(exists), SignedDecimalIntegerLiteral("0")) => exists
    case LessThan(SignedDecimalIntegerLiteral("0"), CountLikeToExistsConverter(exists))    => exists
    case Equals(CountLikeToExistsConverter(exists), SignedDecimalIntegerLiteral("0")) => Not(exists)(exists.position)
    case Equals(SignedDecimalIntegerLiteral("0"), CountLikeToExistsConverter(exists)) => Not(exists)(exists.position)
  })

  override def apply(v: AnyRef): AnyRef = instance(v)

}

case object normalizeExistsPatternExpressions extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[Condition] = Set(
    !noUnnamedNodesAndRelationships,
    PatternExpressionsHaveSemanticInfo, // Looks up type of pattern expressions
    SizeOfCollectRewrittenToCount // Needed so that the COUNT { } > 0 => EXISTS { } rewrite can kick in
  )

  override def postConditions: Set[Condition] = Set(
    containsNoNodesOfType[PatternExpression]()
  )

  override def invalidatedConditions: Set[Condition] = Set(
    // It can invalidate this condition by rewriting things inside WITH/RETURN.
    ProjectionClausesHaveSemanticInfo,
    // This can introduce Not(Not(...))
    PredicatesSimplified
  )

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = normalizeExistsPatternExpressions(semanticState)
}

case object CountLikeToExistsConverter {

  def unapply(expression: Expression): Option[ExistsExpression] = expression match {

    // COUNT { (n)--(m) }
    case ce @ CountExpression(query) =>
      Some(ExistsExpression(query)(
        ce.position,
        ce.computedIntroducedVariables,
        ce.computedScopeDependencies
      ))

    case _ =>
      None
  }
}

case object PatternToQueryConverter {

  def convertPatternToQuery(
    patternElement: PatternElement,
    maybeWhere: Option[Where],
    position: InputPosition
  ): Query = {
    val pattern = Pattern.ForMatch(Seq(
      PatternPartWithSelector(PatternPart.AllPaths()(position), PatternPart(patternElement))
    ))(position)

    SingleQuery(
      Seq(
        Match(optional = false, matchMode = MatchMode.default(position), pattern, Seq.empty, maybeWhere)(position)
      )
    )(position)
  }

}
