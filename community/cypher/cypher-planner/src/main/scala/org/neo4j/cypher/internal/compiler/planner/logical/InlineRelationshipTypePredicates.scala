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
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.ListSet

/**
 * This rewriter moves relationship type predicates from the selection to the patternRelationships.
 * This makes it possible to later rewrite some queries to varExpand instead of ExpandAll, filter and Trail for quantified path patterns.
 * But it also opens up simpler planning for other queries.
 */
case object InlineRelationshipTypePredicates extends PlannerQueryRewriter with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  private case class InlinedRelationship(rel: PatternRelationship, inlinedPredicate: Option[Predicate])

  private def tryToInline(typePredicates: Map[LogicalVariable, (Predicate, Seq[RelTypeName])])(rel: PatternRelationship)
    : InlinedRelationship =
    // if there are types we could inline on that relationship
    typePredicates.get(rel.variable)
      // and that relationship does not have any relationship types yet
      .filter(_ => rel.types.isEmpty)
      .map { case (pred, types) =>
        // then inline the types
        InlinedRelationship(rel.copy(types = types), Some(pred))
      }
      .getOrElse(
        // otherwise just add the relationship as is.
        InlinedRelationship(rel, None)
      )

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = {
    topDown(
      rewriter = Rewriter.lift {
        case qg: QueryGraph =>
          val typePredicates = findRelationshipTypePredicatesPerSymbol(qg)

          val inlinedRelationships = qg.patternRelationships.map(tryToInline(typePredicates))

          qg.withPatternRelationships(inlinedRelationships.map(_.rel))
            .withSelections(qg.selections.copy(predicates =
              qg.selections.predicates -- inlinedRelationships.flatMap(_.inlinedPredicate)
            ))

        case qpp: QuantifiedPathPattern =>
          val typePredicates = findRelationshipTypePredicatesPerSymbol(qpp.asQueryGraph)

          val inlinedRelationships = qpp.patternRelationships.map(tryToInline(typePredicates))

          qpp.copy(
            patternRelationships = inlinedRelationships.map(_.rel),
            selections = qpp.selections.copy(predicates =
              qpp.selections.predicates -- inlinedRelationships.iterator.flatMap(_.inlinedPredicate)
            )
          )
      }
    )
  }

  private def findRelationshipTypePredicatesPerSymbol(qg: QueryGraph)
    : Map[LogicalVariable, (Predicate, Seq[RelTypeName])] = {
    qg.selections.predicates.foldLeft(Map.empty[LogicalVariable, (Predicate, Seq[RelTypeName])]) {
      // WHERE r:REL
      case (acc, pred @ Predicate(_, HasTypes(v: Variable, relTypes))) =>
        acc + (v -> (pred -> relTypes))

      // WHERE r:REL OR r:OTHER_REL
      case (acc, pred @ Predicate(_, ors: Ors)) =>
        ors.exprs.head match {
          case HasTypes(v @ Variable(name), _) =>
            val relTypesOnTheSameVariable = ors.exprs.flatMap {
              case HasTypes(Variable(`name`), relTypes) => relTypes
              case _                                    => ListSet.empty
            }

            // all predicates must refer to the same variable to be equivalent to [r:A|B|C]
            if (relTypesOnTheSameVariable.size == ors.exprs.size) {
              acc + (v -> (pred -> relTypesOnTheSameVariable.toSeq))
            } else {
              acc
            }
          case _ => acc
        }

      case (acc, _) =>
        acc
    }
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This works on the IR
    CompilationContains[PlannerQuery](),
    // We rewrite variables by name, so they need to be unique.
    Namespacer.completed
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
