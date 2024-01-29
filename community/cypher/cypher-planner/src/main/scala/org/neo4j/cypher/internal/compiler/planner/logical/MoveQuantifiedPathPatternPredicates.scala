/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

/**
 * Move QPP predicates into Selections in QueryGraph or Selective Path Pattern.
 */
case object MoveQuantifiedPathPatternPredicates extends PlannerQueryRewriter with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  val rewriter: Rewriter = topDown(
    rewriter = Rewriter.lift {
      case qg: QueryGraph if qg.quantifiedPathPatterns.nonEmpty =>
        val liftedQppPredicates = for {
          qpp <- qg.quantifiedPathPatterns
          predicate <- qpp.selections.predicates
        } yield ForAllRepetitions(qpp, predicate.expr)

        val fixedQpps =
          qg.quantifiedPathPatterns.map(qpp => qpp.copy(selections = Selections.empty))

        qg
          .withQuantifiedPathPatterns(fixedQpps)
          .addPredicates(liftedQppPredicates.toSeq: _*)

      case spp: SelectivePathPattern if spp.allQuantifiedPathPatterns.nonEmpty =>
        val (newConnectionsBuilder, liftedPredicates) =
          spp.pathPattern.connections.foldLeft((
            NonEmptyList.newBuilder[ExhaustiveNodeConnection],
            Set[ForAllRepetitions]()
          )) {
            case ((newNodeConnections, extractedPredicates), nodeConnection) => nodeConnection match {
                case pr: PatternRelationship => (newNodeConnections.addOne(pr), extractedPredicates)
                case qpp: QuantifiedPathPattern =>
                  val foo = for {
                    predicate <- qpp.selections.predicates
                  } yield ForAllRepetitions(qpp, predicate.expr)
                  (newNodeConnections.addOne(qpp.copy(selections = Selections.empty)), extractedPredicates ++ foo)
              }
          }
        val newSelections = Selections(liftedPredicates.flatMap(_.asPredicates))

        spp.copy(
          pathPattern = spp.pathPattern.copy(connections =
            newConnectionsBuilder.result().getOrElse(
              throw new IllegalArgumentException(
                s"Attempt to construct empty non-empty list in ${this.getClass.getSimpleName}"
              )
            )
          ),
          selections = spp.selections ++ newSelections
        )
    }
  )

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter =
    rewriter

  override def preConditions: Set[StepSequencer.Condition] = Set(
    CompilationContains[PlannerQuery](),
    InlineRelationshipTypePredicates.completed
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
