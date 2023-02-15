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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Verify aggregation expressions and make sure there are no ambiguous grouping keys.
 */
case class AmbiguousAggregationAnalysis(features: SemanticFeature*)
    extends VisitorPhase[BaseContext, BaseState] {

  override def visit(from: BaseState, context: BaseContext): Unit = {
    val errors = from.folder.fold(Seq.empty[SemanticErrorDef]) {
      // If we project '*', we don't need to check ambiguity since we group on all available variables.
      case projectionClause: ProjectionClause if !projectionClause.returnItems.includeExisting =>
        _ ++ projectionClause.orderBy.toSeq.flatMap(_.checkIllegalOrdering(projectionClause.returnItems)) ++
            ReturnItems.checkAmbiguousGrouping(projectionClause.returnItems)
    }

    context.errorHandler(errors)
  }

  override def phase: CompilationPhaseTracer.CompilationPhase = SEMANTIC_CHECK

  override def postConditions: Set[StepSequencer.Condition] = Set.empty
}
