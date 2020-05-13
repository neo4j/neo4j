/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlanner
import org.neo4j.cypher.internal.compiler.planner.CheckForUnresolvedTokens
import org.neo4j.cypher.internal.compiler.planner.ResolveTokens
import org.neo4j.cypher.internal.compiler.MultiDatabaseAdministrationCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.SchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.UnsupportedSystemCommand
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.LiteralExtraction
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer

object CompilationPhases {

  // Phase 1
  def parsing(sequencer: String => RewriterStepSequencer,
              innerVariableNamer: InnerVariableNamer,
              compatibilityMode: Boolean = false,
              literalExtraction: LiteralExtraction = IfNoParameter
             ): Transformer[BaseContext, BaseState, BaseState] = {
    if (compatibilityMode) {
      Parsing.adds(BaseContains[Statement]) andThen
        SyntaxDeprecationWarnings(Deprecations.removedFeaturesIn4_0) andThen
        PreparatoryRewriting(Deprecations.removedFeaturesIn4_0) andThen
        SyntaxDeprecationWarnings(Deprecations.V2) andThen
        PreparatoryRewriting(Deprecations.V2) andThen
        SemanticAnalysis(warn = true, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
        AstRewriting(sequencer, literalExtraction, innerVariableNamer = innerVariableNamer)
    } else {
      Parsing.adds(BaseContains[Statement]) andThen
        SyntaxDeprecationWarnings(Deprecations.V2) andThen
        PreparatoryRewriting(Deprecations.V2) andThen
        SemanticAnalysis(warn = true, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
        AstRewriting(sequencer, literalExtraction, innerVariableNamer = innerVariableNamer)
    }
}

  // Phase 2
  val prepareForCaching: Transformer[PlannerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
      ProcedureDeprecationWarnings andThen
      ProcedureWarnings

  // Phase 3
  def planPipeLine(sequencer: String => RewriterStepSequencer, pushdownPropertyReads: Boolean = true): Transformer[PlannerContext, BaseState, LogicalPlanState] =
    SchemaCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        SemanticAnalysis(warn = false, MultipleDatabases) andThen
          Namespacer andThen
          isolateAggregation andThen
          SemanticAnalysis(warn = false, MultipleDatabases) andThen
          Namespacer andThen
          transitiveClosure andThen
          rewriteEqualityToInPredicate andThen
          CNFNormalizer andThen
          LateAstRewriting andThen
          SemanticAnalysis(warn = false, MultipleDatabases) andThen
          ResolveTokens andThen
          CreatePlannerQuery.adds(CompilationContains[UnionQuery]) andThen
          OptionalMatchRemover andThen
          QueryPlanner.adds(CompilationContains[LogicalPlan]) andThen
          PlanRewriter(sequencer) andThen
          InsertCachedProperties(pushdownPropertyReads) andThen
          If((s: LogicalPlanState) => s.query.readOnly)(
            CheckForUnresolvedTokens
          )
      )

  // Alternative Phase 3
  def systemPipeLine: Transformer[PlannerContext, BaseState, LogicalPlanState] =
    RewriteProcedureCalls andThen
    MultiDatabaseAdministrationCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        UnsupportedSystemCommand
      )
}
