/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.AmbiguousNamesDisambiguated
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter copies predicates from the start and end nodes of a QPP to the connected adjacent nodes.
 * This will allow to reduce cardinality before the Trail plan.
 * Especially for bare QPP MATCH clauses, this can also enable index and label scan usage.
 */
case object MoveQuantifiedPathPatternPredicatesToConnectedNodes extends PlannerQueryRewriter with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  case object QuantifiedPathPatternPredicatesMovedToConnectedNodes extends StepSequencer.Condition

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = {
    topDown(
      rewriter = Rewriter.lift {
        case qg: QueryGraph =>
          val predicatesForOuterSelections = qg.quantifiedPathPatterns.flatMap { qpp =>
            val (left, right) = qpp.nodes
            val start = qpp.leftBinding.inner
            val end = qpp.rightBinding.inner

            qpp.pattern.selections.predicates.foldLeft(Set.empty[Predicate]) {
              case (acc, predicate) =>
                val deps = predicate.dependencies

                // We can only copy a predicate if it depends only on arguments and either the start or the end node.
                // We could theoretically also copy a predicate with dependencies on both start and end node,
                // but that would currently mostly result in a filter _after_ the Trail plan, not filtering out anything new.
                // What we are trying to achieve instead as a filter _before_ the Trail plan.
                val okDependencies = deps.subsetOf(qpp.pattern.argumentIds + start) ||
                  deps.subsetOf(qpp.pattern.argumentIds + end)

                // IR Expressions can also not easily be rewritten, since they contain variables as simple Strings.
                val noIRExpressions = predicate.folder.treeFindByClass[IRExpression].isEmpty

                if (okDependencies && noIRExpressions) {
                  val rewritttenPredicate: Predicate =
                    rewritePredicate(predicate, start -> left, end -> right, context.cancellationChecker)
                  acc + rewritttenPredicate
                } else {
                  acc
                }
            }
          }

          val newSelections = qg.selections ++ Selections(predicatesForOuterSelections)
          qg.withSelections(newSelections)
      },
      cancellation = context.cancellationChecker
    )
  }

  private def rewritePredicate(
    p: Predicate,
    startRewrite: (String, String),
    endRewrite: (String, String),
    cancellationChecker: CancellationChecker
  ): Predicate = {
    val containsStart = p.dependencies(startRewrite._1)
    val containsEnd = p.dependencies(endRewrite._1)
    val newDependencies = Set.empty[String] union
      (if (containsStart) Set(startRewrite._2) else Set.empty) union
      (if (containsEnd) Set(endRewrite._2) else Set.empty)

    val rewrittenDependencies = p.dependencies - startRewrite._1 - endRewrite._1 union newDependencies

    val rewrittenExpression = p.expr.endoRewrite(topDown(
      rewriter = Rewriter.lift {
        case v @ Variable(varName) if varName == startRewrite._1 => v.copy(startRewrite._2)(v.position)
        case v @ Variable(varName) if varName == endRewrite._1   => v.copy(endRewrite._2)(v.position)
      },
      cancellation = cancellationChecker
    ))

    Predicate(rewrittenDependencies, rewrittenExpression)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This works on the IR
    CompilationContains[PlannerQuery],
    // We rewrite variables by name, so they need to be unique.
    AmbiguousNamesDisambiguated
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(QuantifiedPathPatternPredicatesMovedToConnectedNodes)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
