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

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPart.AllShortestPaths
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.PatternPart.ShortestGroups
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrites selectors ALL SHORTEST and SHORTEST k GROUPS to ALL,
 * if the path pattern following them is of fixed length.
 */
case object FixedLengthShortestToAllRewriter extends StepSequencer.Step with DefaultPostCondition
    with ASTRewriterFactory {

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance

  override def preConditions: Set[StepSequencer.Condition] = Set()

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val instance: Rewriter = topDown(Rewriter.lift {
    case p @ PatternPartWithSelector(sel @ AllShortest(), part) if part.isFixedLength =>
      p.copy(selector = AllPaths()(sel.position))
    case p @ PatternPartWithSelector(sel: SelectiveSelector, SingleNode()) =>
      p.copy(selector = AllPaths()(sel.position))
  })

  /**
   * Matcher for ALL SHORTEST and SHORTEST k GROUPS.
   * Both of these can be rewritten to ALL, if the pattern is of fixed length.
   */
  private object AllShortest {

    def unapply(selector: Selector): Boolean = selector match {
      case AllShortestPaths() => true
      case ShortestGroups(_)  => true
      case _                  => false
    }
  }

  /**
   * Matcher for a [[NonPrefixedPatternPart]] that contains only a single [[NodePattern]].
   */
  private object SingleNode {

    def unapply(nonPrefixedPatternPart: NonPrefixedPatternPart): Boolean = {
      nonPrefixedPatternPart.element match {
        case _: NodePattern => true
        case _              => false
      }
    }
  }

}
