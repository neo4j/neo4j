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

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedPatternElementsInMatch
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedPatternElementsInPatternComprehension
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AllNameGenerators
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CypherType

case class nameAllPatternElements(allNameGenerators: AllNameGenerators) extends Rewriter {

  override def apply(that: AnyRef): AnyRef = namingRewriter.apply(that)

  private def namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if pattern.variable.isEmpty =>
      val syntheticName = allNameGenerators.nodeNameGenerator.nextName
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if pattern.variable.isEmpty  =>
      val syntheticName = allNameGenerators.relNameGenerator.nextName
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
  }, stopper = {
    case _: ShortestPathExpression => true
    case _ => false
  })
}

object nameAllPatternElements extends StepSequencer.Step with ASTRewriterFactory {
  override def getRewriter(innerVariableNamer: InnerVariableNamer,
                           semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory,
                           allNameGenerators: AllNameGenerators): Rewriter = nameAllPatternElements(allNameGenerators)

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedPatternElementsInMatch,
    noUnnamedPatternElementsInPatternComprehension
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo, // It can invalidate this condition by rewriting things inside WITH/RETURN.
    PatternExpressionsHaveSemanticInfo, // It can invalidate this condition by rewriting things inside PatternExpressions.
  )
}
