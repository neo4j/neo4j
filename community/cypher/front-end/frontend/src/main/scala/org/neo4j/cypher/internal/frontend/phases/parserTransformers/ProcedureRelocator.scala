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

import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.NoOp
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp

case object ProceduresRelocated extends Condition

/**
 * Rewrites certain procedures to a different implementation in graph engine.
 * For example: CALL db.labels() ==> CALL internal.virtual_graph.override.db.labels()
 */
case object ProcedureRelocator extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  private val supportedProcedures: Set[String] = Set(
    "apoc.meta.schema",
    "apoc.meta.stats",
    "apoc.convert.toJson",
    "db.labels",
    "db.relationshipTypes",
    "db.schema.nodeTypeProperties",
    "db.schema.relTypeProperties",
    "db.schema.visualization",
    "db.propertyKeys"
  )

  private val rewriter = bottomUp(Rewriter.lift {
    case resolvedFunctionInvocation: ResolvedFunctionInvocation
      if supportedProcedures.contains(resolvedFunctionInvocation.functionName.fullName) =>
      asGraphEngineOverride(resolvedFunctionInvocation.asUnresolvedFunction)
    case functionInvocation: FunctionInvocation
      if supportedProcedures.contains(functionInvocation.functionName.fullName) =>
      asGraphEngineOverride(functionInvocation)
    case unresolvedCall: UnresolvedCall
      if supportedProcedures.contains(unresolvedCall.procedureName.fullName) =>
      asGraphEngineOverride(unresolvedCall)
    case resolvedCall: ResolvedCall[_]
      if supportedProcedures.contains(resolvedCall.procedureName.fullName) =>
      asGraphEngineOverride(resolvedCall.asUnresolvedCall)
  })

  private def asGraphEngineOverride(call: UnresolvedCall): UnresolvedCall = {
    val name = call.procedureName
    call.copy(procedureName =
      name.copy(namespace =
        Namespace(List("internal", "virtual_graph", "override") ++ name.namespace.parts)(name.position)
      )(name.position)
    )(call.position)
  }

  private def asGraphEngineOverride(functionInvocation: FunctionInvocation): FunctionInvocation = {
    val name = functionInvocation.functionName
    functionInvocation.copy(functionName =
      name.copy(namespace =
        Namespace(List("internal", "virtual_graph", "override") ++ name.namespace.parts)(name.position)
      )(name.position)
    )(functionInvocation.position)
  }

  override def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(rewriter))

  override def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

  override def preConditions: Set[StepSequencer.Condition] = Set(ShadowedFunctionsUnresolved)

  override def postConditions: Set[StepSequencer.Condition] = Set(ProceduresRelocated)

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    if (config.enabledVirtualGraph) this else NoOp()
}
