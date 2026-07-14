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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.topDown

/**
 * Rename variables so they are all unique.
 */
case object Namespacer extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {
  type VariableRenamings = Map[Ref[LogicalVariable], LogicalVariable]

  override def phase: CompilationPhase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val withProjectedUnions = from.statement().endoRewrite(projectUnions)
    // projectUnions rewrites the statement before scope consumption, so we must
    // re-run ScopeSurveyor to get WorkingScope aligned with the rewritten tree.
    val surveyed = ScopeSurveyor.process(from.withStatement(withProjectedUnions), context)

    val renamings: VariableRenamings =
      surveyed.scopeState().workingScope
        .getSymbolGroups(surveyed.anonymousVariableNameGenerator)
        .flatMap(_.getRenamedUses).flatten.toMap

    if (renamings.isEmpty) surveyed
    else {
      val rewriter = renamingRewriter(renamings)
      surveyed
        .withStatement(surveyed.statement().endoRewrite(rewriter))
        .withSemanticTable(surveyed.semanticTable().replaceExpressions(rewriter))
    }
  }

  def projectUnions: Rewriter = {
    // This needs to be topDown so that Unions do net get copied before being replaced by a ProjectingUnion,
    // otherwise we create new copies of the unionMapping variables which are then unknown to the semantic state.
    topDown(Rewriter.lift {
      case u: UnionAll      => ProjectingUnionAll(u.lhs, u.rhs, u.unionMappings)(u.position)
      case u: UnionDistinct => ProjectingUnionDistinct(u.lhs, u.rhs, u.unionMappings)(u.position)
    })
  }

  private def renamingRewriter(renamings: VariableRenamings): Rewriter = {
    def rename(v: LogicalVariable) = renamings.getOrElse(Ref(v), v)
    inSequence(
      bottomUp(Rewriter.lift {
        case item @ ProcedureResultItem(None, v: Variable) if renamings.contains(Ref(v)) =>
          item.copy(output = Some(ProcedureOutput(v.name)(v.position)))(item.position)
      }),
      bottomUp(Rewriter.lift {
        case v: Variable => rename(v)
        case e: ExpressionWithComputedDependencies =>
          val newIntroducedVariables = e.introducedVariables.map(rename)
          val newScopeDependencies = e.scopeDependencies.map(rename)
          e.withComputedIntroducedVariables(newIntroducedVariables).withComputedScopeDependencies(newScopeDependencies)
      })
    )
  }

  override def preConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def postConditions: Set[StepSequencer.Condition] = Set(
    ContainsNoNodesOfType[UnionAll](),
    ContainsNoNodesOfType[UnionDistinct](),
    completed
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    SemanticInfoAvailable + UpToDateScopes // Introduces new AST nodes

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[BaseContext, BaseState, BaseState] = this
}
