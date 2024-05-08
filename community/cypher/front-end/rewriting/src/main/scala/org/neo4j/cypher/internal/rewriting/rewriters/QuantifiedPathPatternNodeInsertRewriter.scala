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
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

/**
 * A Quantified Path Pattern should always have a node or relationship chain surrounding it,
 * a user may omit these in their query whilst the planner expects them, this rewriter will therefore add
 * a filler Node in
 */
case object QuantifiedPathPatternNodeInsertRewriter extends StepSequencer.Step with DefaultPostCondition
    with ASTRewriterFactory {

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance

  private val filler = NodePattern(None, None, None, None)(InputPosition.NONE)

  override def preConditions: Set[StepSequencer.Condition] = Set()

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    SemanticInfoAvailable ++ Set(
      // we potentially introduce unnamed pattern nodes
      noUnnamedNodesAndRelationships
    )

  val instance: Rewriter = topDown(Rewriter.lift {
    // A `PatternElement` occurs only in `ShortestPaths` and `PatternPart`
    // However, `ShortestPaths` may only contain `RelationshipChain`s.
    case PathPatternPart(p @ PathConcatenation(factors)) =>
      val newFactors = padQuantifiedPathPatterns(factors)
      PathPatternPart(PathConcatenation(newFactors)(p.position))

    case PathPatternPart(q: QuantifiedPath) =>
      PathPatternPart(PathConcatenation(Seq(filler, q, filler))(q.position))
  })

  private def padQuantifiedPathPatterns(factors: Seq[PathFactor]): Seq[PathFactor] = {
    val newFactors = (None +: factors.map(Some(_)) :+ None)
      .sliding(2).flatMap {
        case Seq(None, Some(q: QuantifiedPath))                    => Seq(filler, q)
        case Seq(Some(_: QuantifiedPath), None)                    => Seq(filler)
        case Seq(_, None)                                          => Seq()
        case Seq(Some(_: QuantifiedPath), Some(q: QuantifiedPath)) => Seq(filler, q)
        case Seq(_, Some(second))                                  => Seq(second)
        // Sliding should always return sequence of at most 2 elements, we know our list is
        // at least 3 elements so we cannot hit the 1 element case
        case _ => throw new IllegalStateException()
      }
    newFactors.toSeq
  }
}
