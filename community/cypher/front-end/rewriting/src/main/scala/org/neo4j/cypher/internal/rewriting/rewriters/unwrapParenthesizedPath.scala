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
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * We parse parenthesized path patterns and represent them in the AST as they are, as we have the information anyways and will need it later on.
 * As of now, we do not allow anything particular useful to be done with them which is why we can avoid treating them in the rest of the system
 * and remove them as part of this rewriter.
 *
 * This will probably change when we allow
 *   - juxtaposing (non-quantified) parenthesized path patterns with other parts of the query
 *
 */
case object unwrapParenthesizedPath extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable + AndRewrittenToAnds

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = {
    unwrapParenthesizedPath.instance
  }

  val instance: Rewriter = bottomUp(
    Rewriter.lift {
      case m @ Match(_, _, pattern, _, where) =>
        val newOuterExp = extractPredicatesAndMergeWhereExpressions(pattern, where.map(_.expression), m.position)

        val newWhere = newOuterExp.map {
          Where(_)(where.fold(m.position)(_.position))
        }

        m.copy(
          pattern = replaceParenthesizedPaths(pattern),
          where = newWhere
        )(m.position)
      case m @ Merge(pattern, _, where) =>
        val newOuterExp = extractPredicatesAndMergeWhereExpressions(pattern, where.map(_.expression), m.position)

        val newWhere = newOuterExp.map {
          Where(_)(where.fold(m.position)(_.position))
        }

        m.copy(
          pattern = replaceParenthesizedPaths(pattern),
          where = newWhere
        )(m.position)

      case outer @ ParenthesizedPath(part, outerWhere) =>
        val newOuterWhere = extractPredicatesAndMergeWhereExpressions(part, outerWhere, outer.position)

        outer.copy(
          replaceParenthesizedPaths(part),
          newOuterWhere
        )(outer.position)

      case outer @ QuantifiedPath(part, _, outerWhere, _) =>
        val newOuterWhere = extractPredicatesAndMergeWhereExpressions(part, outerWhere, outer.position)

        outer.copy(
          part = replaceParenthesizedPaths(part),
          optionalWhereExpression = newOuterWhere
        )(outer.position)
    }
  )

  private def extractPredicatesAndMergeWhereExpressions(
    pattern: AnyRef,
    outerWhere: Option[Expression],
    position: InputPosition
  ): Option[Expression] = {
    val innerPredicates = extractPredicates(pattern)
    val newPredicates = innerPredicates ++ outerWhere
    newPredicates.reduceOption(And(_, _)(position))
  }

  private def extractPredicates(pattern: AnyRef): Seq[Expression] = pattern.folder.treeFold(Vector.empty[Expression]) {
    // Extract Predicates from ParenthesizedPaths
    case ParenthesizedPath(_, Some(where)) => acc => TraverseChildren(acc :+ where)
    // Do not traverse down into pattern parts with selectors (!= AllPaths)
    case PatternPartWithSelector(_: SelectiveSelector, _) => acc => SkipChildren(acc)
    // Traverse the rest
    case _ => acc => TraverseChildren(acc)
  }

  private def replaceParenthesizedPaths[T <: AnyRef](pattern: T): T = pattern.endoRewrite(
    inSequence(
      replaceParenthesizedPathsInPatternsWithNoSelector,
      replaceParenthesizedAnonymousPathsWithNoPredicates
    )
  )

  private def replaceParenthesizedPathsInPatternsWithNoSelector: Rewriter = bottomUp(
    Rewriter.lift {
      case p: ParenthesizedPath => p.part.element
    },
    stopper = {
      case PatternPartWithSelector(_: SelectiveSelector, _) => true
      case _                                                => false
    }
  )

  private def replaceParenthesizedAnonymousPathsWithNoPredicates: Rewriter = bottomUp(
    Rewriter.lift {
      case ParenthesizedPath(part: AnonymousPatternPart, None) =>
        // Removing the parenthesized path is not sound if the nested pattern is a sub-path assignment in which case we would lose the sub-path variable
        part.element
    }
  )
}
