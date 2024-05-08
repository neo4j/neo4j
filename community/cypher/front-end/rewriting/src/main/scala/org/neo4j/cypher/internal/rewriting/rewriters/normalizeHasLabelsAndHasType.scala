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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

trait HasLabelsAndHasTypeNormalizer extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private def rewrite(expression: Expression): Expression = expression match {
    case p @ HasLabelsOrTypes(e, labels) if isNode(e) =>
      HasLabels(e, labels.map(l => LabelName(l.name)(l.position)))(p.position)
    case p @ HasLabelsOrTypes(e, labels) if isRelationship(e) =>
      HasTypes(e, labels.map(l => RelTypeName(l.name)(l.position)))(p.position)
    case p @ HasALabelOrType(e) if isNode(e) =>
      HasALabel(e)(p.position)
    case p @ HasALabelOrType(e) if isRelationship(e) =>
      True()(p.position)
    case e =>
      e
  }

  protected val instance: Rewriter = topDown(Rewriter.lift {
    case e: Expression => rewrite(e)
  })

  def isNode(expr: Expression): Boolean
  def isRelationship(expr: Expression): Boolean
}

case class normalizeHasLabelsAndHasType(semanticState: SemanticState) extends HasLabelsAndHasTypeNormalizer {

  def isNode(expr: Expression): Boolean =
    semanticState.expressionType(expr).actual == CTNode.invariant

  def isRelationship(expr: Expression): Boolean =
    semanticState.expressionType(expr).actual == CTRelationship.invariant
}

case object normalizeHasLabelsAndHasType extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    normalizePredicates.completed // We first need to extract predicates from nodes and relationships
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = normalizeHasLabelsAndHasType(semanticState)
}
