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
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalFunctionScopeSignature
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.LocalFunction
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.topDown

case object LocalFunctionsResolved extends Condition

case object ResolveLocalFunctions extends StatementRewriter with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def instance(from: BaseState, context: BaseContext): Rewriter = {
    val recordedScopes = from.scopeState().recordedScopes
    topDown(
      Rewriter.lift {
        case fi: FunctionInvocation if fi.maybeLocalFunction.isEmpty =>
          recordedScopes.get(Ref(fi)).flatMap(ws =>
            ws.incoming.localCallables.collectFirst {
              case sig @ LocalFunctionScopeSignature(name, _, _, _) if name.fullNameEqual(fi.functionName) => sig
            }
          ) match {
            case None => fi
            case Some(sig) =>
              val (optionalInputFieldSignature, mandatoryInputFieldSignature) =
                sig.inputSignature.toIndexedSeq.partition(fs => fs.hasDefault)

              fi.copy(maybeLocalFunction =
                Some(LocalFunction(
                  functionName = fi.functionName,
                  parameterTypes = mandatoryInputFieldSignature.map(p => p.name -> p.getType),
                  optionalParameterTypes = optionalInputFieldSignature.map(p => p.name -> p.getType),
                  defaultArguments = optionalInputFieldSignature.map(_.default.get),
                  outputSignature = sig.outputSignature
                ))
              )(fi.position)
          }
      }
    )
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement](), UpToDateScopes)

  override def postConditions: Set[StepSequencer.Condition] = Set(LocalFunctionsResolved)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable + UpToDateScopes

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    ResolveLocalFunctions
}
