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

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FilteringExpression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionAreWrappedInExists
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType



/**
 * Adds an exists() around any pattern expression that is expected to produce a boolean e.g.
 *
 * MATCH (n) WHERE (n)-->(m) RETURN n
 *
 * is rewritten to
 *
 * MATCH (n) WHERE EXISTS((n)-->(m)) RETURN n
 *
 * Rewrite equivalent expressions with `size` or `length` to `exists`.
 * This rewrite normalizes this cases and make it easier to plan correctly.
 *
 * [[simplifyPredicates]]  takes care of rewriting the Not(Not(Exists(...))) which can be introduced by this rewriter.
 *
 * This rewriter needs to run before [[namePatternElements]], which rewrites pattern expressions. Otherwise we don't find them in the semantic table.
 */
case class normalizeExistsPatternExpressions(semanticState: SemanticState) extends Rewriter {

  override def apply(v: AnyRef): AnyRef = {
    val replacements: IdentityMap[AnyRef, AnyRef] = computeReplacements(v)
    createRewriter(replacements).apply(v)
  }

  private def computeReplacements(v: AnyRef): IdentityMap[AnyRef, AnyRef] = {
    v.treeFold(IdentityMap.empty[AnyRef, AnyRef]) {
      // find replacements for pattern expressions, ONLY if they are used in supported places (e.g. inside WHERE clause or inside a pattern comprehension)
      case w: Where =>
        acc => TraverseChildren(patternExpressionAsBooleanReplacements(w, acc))
      case e:FilteringExpression if e.innerPredicate.isDefined =>
        acc => SkipChildren(patternExpressionAsBooleanReplacements(e, acc))
      case p@PatternComprehension(_, _, Some(predicate), _) =>
        acc => SkipChildren(patternExpressionAsBooleanReplacements(p, acc))

      // other replacements for pattern expressions
      case g@GreaterThan(Size(p: PatternExpression), SignedDecimalIntegerLiteral("0")) =>
        acc => SkipChildren(acc.updated(g, Exists(p)(p.position)))
      case l@LessThan(SignedDecimalIntegerLiteral("0"), Size(p: PatternExpression)) =>
        acc => SkipChildren(acc.updated(l, Exists(p)(p.position)))
      case e@Equals(Size(p: PatternExpression), SignedDecimalIntegerLiteral("0")) =>
        acc => SkipChildren(acc.updated(e, Not(Exists(p)(p.position))(p.position)))
      case e@Equals(SignedDecimalIntegerLiteral("0"), Size(p: PatternExpression)) =>
        acc => SkipChildren(acc.updated(e, Not(Exists(p)(p.position))(p.position)))

      case _ =>
        acc => TraverseChildren(acc)
    }
  }

  private def patternExpressionAsBooleanReplacements(v: AnyRef, accumulator: IdentityMap[AnyRef, AnyRef]): IdentityMap[AnyRef, AnyRef] = {
    v.treeFold(accumulator) {
      case p: PatternExpression if semanticState.expressionType(p).expected.contains(symbols.CTBoolean.invariant) =>
        acc => SkipChildren(acc.updated(p, Exists(p)(p.position)))
      case _ =>
        acc => TraverseChildren(acc)
    }
  }

  private def createRewriter(replacements: IdentityMap[AnyRef, AnyRef]): Rewriter = {
    bottomUp(Rewriter.lift {
      case that => replacements.getOrElse(that, that)
    })
  }
}

object normalizeExistsPatternExpressions extends StepSequencer.Step with ASTRewriterFactory {
  override def preConditions: Set[Condition] = Set(
    PatternExpressionsHaveSemanticInfo // Looks up type of pattern expressions
  )

  override def postConditions: Set[Condition] = Set(PatternExpressionAreWrappedInExists)

  // TODO capture the dependency with simplifyPredicates
  override def invalidatedConditions: Set[Condition] = Set(
    ProjectionClausesHaveSemanticInfo // It can invalidate this condition by rewriting things inside WITH/RETURN.
  )

  override def getRewriter(innerVariableNamer: InnerVariableNamer,
                           semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory): Rewriter = normalizeExistsPatternExpressions(semanticState)
}