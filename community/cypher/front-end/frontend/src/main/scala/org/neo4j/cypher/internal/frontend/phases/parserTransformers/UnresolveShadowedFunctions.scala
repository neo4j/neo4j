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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.notification.DeprecatedFunctionNamespaceUsed
import org.neo4j.cypher.internal.notification.DeprecatedProcedureNamespaceUsed
import org.neo4j.cypher.internal.notification.ShadowingInternalFunction
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp

case object ShadowedFunctionsUnresolved extends Condition

case object UnresolveShadowedFunctions extends StatementRewriter with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def instance(from: BaseState, context: BaseContext): Rewriter = bottomUp(
    Rewriter.lift {
      case fi: FunctionInvocation
        if context.shadowedFunctions.contains(fi.name) && fi.maybeLocalFunction.isEmpty =>
        val warning = fi.function match {
          case UnresolvedFunction => Some(DeprecatedFunctionNamespaceUsed(fi.position, fi.name))
          case _                  => Some(ShadowingInternalFunction(fi.position, fi.name))
        }
        warning.foreach(context.notificationLogger.log(_))
        fi.copy(isShadowed = true)(fi.position)
      case pc: UnresolvedCall if context.shadowedFunctions.contains(pc.fullName) =>
        context.notificationLogger.log(DeprecatedProcedureNamespaceUsed(pc.position, pc.fullName))
        pc
    }
  )

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement](), LocalFunctionsResolved)

  override def postConditions: Set[StepSequencer.Condition] = Set(ShadowedFunctionsUnresolved)

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    SemanticInfoAvailable ++ Set(UpToDateScopes, FunctionInvocationsResolved)

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    UnresolveShadowedFunctions
}
