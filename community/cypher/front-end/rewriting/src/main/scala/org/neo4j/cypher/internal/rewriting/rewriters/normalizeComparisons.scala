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

import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNamedPathOnlyForShortestPath
import org.neo4j.cypher.internal.rewriting.conditions.noReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object OnlySingleHasLabels extends StepSequencer.Condition

case object normalizeComparisons extends StepSequencer.Step with ASTRewriterFactory {

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = instance

  override def preConditions: Set[StepSequencer.Condition] = Set(
    normalizeHasLabelsAndHasType.completed, // These have to have been rewritten to HasLabels / HasTypes at this point
    !containsNamedPathOnlyForShortestPath // this rewriter will not achieve 'noReferenceEqualityAmongVariables' if projectNamedPaths run first
  )

  override def postConditions: Set[StepSequencer.Condition] =
    Set(OnlySingleHasLabels, noReferenceEqualityAmongVariables)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val instance: Rewriter = topDown(Rewriter.lift {
    case c @ NotEquals(lhs, rhs) =>
      NotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ Equals(lhs, rhs) =>
      Equals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ LessThan(lhs, rhs) =>
      LessThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ LessThanOrEqual(lhs, rhs) =>
      LessThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ GreaterThan(lhs, rhs) =>
      GreaterThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ GreaterThanOrEqual(lhs, rhs) =>
      GreaterThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ InvalidNotEquals(lhs, rhs) =>
      InvalidNotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ HasLabels(expr, labels) if labels.size > 1 =>
      val hasLabels = labels.map(l => HasLabels(expr.endoRewrite(copyVariables), Seq(l))(c.position))
      Ands(hasLabels)(c.position)
    case c @ HasTypes(expr, types) if types.size > 1 =>
      val hasTypes = types.map(t => HasTypes(expr.endoRewrite(copyVariables), Seq(t))(c.position))
      Ands(hasTypes)(c.position)
    case c @ IsNull(lhs) =>
      IsNull(lhs.endoRewrite(copyVariables))(c.position)
    case c @ IsNotNull(lhs) =>
      IsNotNull(lhs.endoRewrite(copyVariables))(c.position)
    case c @ IsTyped(lhs, cypherType) =>
      IsTyped(lhs.endoRewrite(copyVariables), cypherType)(c.position)
    case c @ IsNotTyped(lhs, cypherType) =>
      IsNotTyped(lhs.endoRewrite(copyVariables), cypherType)(c.position)
    case c @ IsNormalized(lhs, normalForm) =>
      IsNormalized(lhs.endoRewrite(copyVariables), normalForm)(c.position)
    case c @ IsNotNormalized(lhs, normalForm) =>
      IsNotNormalized(lhs.endoRewrite(copyVariables), normalForm)(c.position)
    case c @ StartsWith(lhs, rhs) =>
      StartsWith(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ EndsWith(lhs, rhs) =>
      EndsWith(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ Contains(lhs, rhs) =>
      Contains(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ In(lhs, rhs) =>
      In(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c @ RegexMatch(lhs, rhs) =>
      RegexMatch(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
  })
}
