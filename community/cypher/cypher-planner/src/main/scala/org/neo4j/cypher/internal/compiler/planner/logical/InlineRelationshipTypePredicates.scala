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
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.AmbiguousNamesDisambiguated
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.ListSet

/**
 * This rewriter moves relationship type predicates from the selection to the patternRelationships.
 * This makes it possible to later rewrite some queries to varExpand instead of ExpandAll, filter and Trail for quantified path patterns.
 * But it also opens up simpler planning for other queries.
 */
case object InlineRelationshipTypePredicates extends PlannerQueryRewriter with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  case object RelationshipTypePredicatesInlined extends StepSequencer.Condition
  final case class Result(newPatternRelationships: Seq[PatternRelationship], inlinedPredicates: Set[Predicate])

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = {
    topDown(
      rewriter = Rewriter.lift {
        case qg: QueryGraph =>
          val typePredicates = findRelationshipTypePredicatesPerSymbol(qg)

          val result = inlineRelationshipTypes(qg.patternRelationships, typePredicates)

          qg.withPatternRelationships(result.newPatternRelationships.toSet)
            .withSelections(qg.selections.copy(predicates = qg.selections.predicates -- result.inlinedPredicates))

        case qpp: QuantifiedPathPattern =>
          val typePredicates = findRelationshipTypePredicatesPerSymbol(qpp.asQueryGraph)

          val result = inlineRelationshipTypes(qpp.patternRelationships, typePredicates)

          qpp.copy(
            patternRelationships = result.newPatternRelationships,
            selections = qpp.selections.copy(predicates = qpp.selections.predicates -- result.inlinedPredicates)
          )
      }
    )
  }

  private def inlineRelationshipTypes(
    patternRelationships: Iterable[PatternRelationship],
    typePredicates: Map[String, (Predicate, Seq[RelTypeName])]
  ): Result = {
    patternRelationships.foldLeft(Result(Seq.empty, Set.empty)) {
      case (Result(newPatternRelationships, inlinedPredicates), rel) =>
        // if there are types we could inline on that relationship
        typePredicates.get(rel.name)
          // and that relationship does not have any relationship types yet
          .filter(_ => rel.types.isEmpty)
          .map { case (pred, types) =>
            // then inline the types
            Result(newPatternRelationships :+ rel.copy(types = types), inlinedPredicates + pred)
          }
          .getOrElse(
            // otherwise just add the relationship as is.
            Result(newPatternRelationships :+ rel, inlinedPredicates)
          )
    }
  }

  private def findRelationshipTypePredicatesPerSymbol(qg: QueryGraph): Map[String, (Predicate, Seq[RelTypeName])] = {
    qg.selections.predicates.foldLeft(Map.empty[String, (Predicate, Seq[RelTypeName])]) {
      // WHERE r:REL
      case (acc, pred @ Predicate(_, HasTypes(Variable(name), relTypes))) =>
        acc + (name -> (pred -> relTypes))

      // WHERE r:REL OR r:OTHER_REL
      case (acc, pred @ Predicate(_, ors: Ors)) =>
        ors.exprs.head match {
          case HasTypes(Variable(name), _) =>
            val relTypesOnTheSameVariable = ors.exprs.flatMap {
              case HasTypes(Variable(`name`), relTypes) => relTypes
              case _                                    => ListSet.empty
            }

            // all predicates must refer to the same variable to be equivalent to [r:A|B|C]
            if (relTypesOnTheSameVariable.size == ors.exprs.size) {
              acc + (name -> (pred -> relTypesOnTheSameVariable.toSeq))
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
    CompilationContains[PlannerQuery],
    // We rewrite variables by name, so they need to be unique.
    AmbiguousNamesDisambiguated
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(RelationshipTypePredicatesInlined)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
