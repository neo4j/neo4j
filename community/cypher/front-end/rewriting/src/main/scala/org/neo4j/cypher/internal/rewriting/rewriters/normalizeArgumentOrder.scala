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
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.functions.Function.isIdFunction
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.normalizedEqualsArguments
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object ArgumentOrderInComparisonsNormalized extends StepSequencer.Condition

case object normalizeArgumentOrder extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    containsNoNodesOfType[NotEquals]() // NotEquals must have been rewritten to Equals
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    ArgumentOrderInComparisonsNormalized,
    normalizedEqualsArguments
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val instance: Rewriter = topDown(Rewriter.lift {

    // move id(n) on equals to the left
    case predicate @ Equals(func @ FunctionInvocation(_, _, _, _, _, _), _) if isIdFunction(func) =>
      predicate

    case predicate @ Equals(lhs, rhs @ FunctionInvocation(_, _, _, _, _, _)) if isIdFunction(rhs) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    // move n.prop on equals to the left
    case predicate @ Equals(Property(_, _), _) =>
      predicate

    case predicate @ Equals(lhs, rhs @ Property(_, _)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    case inequality: InequalityExpression =>
      val lhsIsProperty = inequality.lhs.isInstanceOf[Property]
      val rhsIsProperty = inequality.rhs.isInstanceOf[Property]
      if (!lhsIsProperty && rhsIsProperty) {
        inequality.swapped
      } else {
        inequality
      }
  })

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = instance
}
