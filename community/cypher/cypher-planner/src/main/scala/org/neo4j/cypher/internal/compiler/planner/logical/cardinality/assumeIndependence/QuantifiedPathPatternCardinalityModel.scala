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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Selectivity

import scala.collection.mutable

trait QuantifiedPathPatternCardinalityModel extends NodeCardinalityModel with PatternRelationshipCardinalityModel {

  def getQuantifiedPathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    quantifiedPathPattern: QuantifiedPathPattern,
    uniqueRelationships: Set[String]
  ): Cardinality = {
    val predicates = QuantifiedPathPatternPredicates.partitionSelections(labelInfo, quantifiedPathPattern.selections)

    lazy val labelsOnFirstNode =
      predicates.labelsOnNodes(
        quantifiedPathPattern.leftBinding.outer.name,
        quantifiedPathPattern.leftBinding.inner.name
      )
    lazy val labelsOnLastNode =
      predicates.labelsOnNodes(
        quantifiedPathPattern.rightBinding.outer.name,
        quantifiedPathPattern.rightBinding.inner.name
      )

    lazy val otherPredicatesSelectivity: Selectivity =
      context.predicatesSelectivityWithExtraRelTypeInfo(
        labelInfo = predicates.allLabelInfo,
        extraRelTypeInfo = quantifiedPathPattern.patternRelationships.collect {
          case PatternRelationship(rel, _, _, Seq(relType), _) => rel -> relType
        }.toMap,
        predicates = predicates.otherPredicates
      )

    val patternCardinality =
      RepetitionCardinalityModel
        .quantifiedPathPatternRepetitionAsRange(quantifiedPathPattern.repetition)
        .view
        .map {
          case 0 =>
            getEmptyPathPatternCardinality(
              context,
              predicates.allLabelInfo,
              quantifiedPathPattern.left.name,
              quantifiedPathPattern.right.name
            )

          case 1 =>
            val singleIterationLabels =
              predicates.allLabelInfo
                .updated(quantifiedPathPattern.leftBinding.inner, labelsOnFirstNode)
                .updated(quantifiedPathPattern.rightBinding.inner, labelsOnLastNode)
            val uniquenessSelectivity = DEFAULT_REL_UNIQUENESS_SELECTIVITY ^ predicates.differentRelationships.size
            val patternCardinality =
              getPatternRelationshipsCardinality(
                context,
                singleIterationLabels,
                quantifiedPathPattern.patternRelationships
              )
            patternCardinality * uniquenessSelectivity * otherPredicatesSelectivity

          case i =>
            val labelsOnJunctionNode = predicates.labelsOnNodes(
              quantifiedPathPattern.leftBinding.inner.name,
              quantifiedPathPattern.rightBinding.inner.name
            )

            val junctionNodeCardinality =
              resolveNodeLabels(context, labelsOnJunctionNode)
                .map(getLabelsCardinality(context, _))
                .getOrElse(Cardinality.EMPTY)

            val firstIterationLabels =
              predicates.allLabelInfo
                .updated(quantifiedPathPattern.leftBinding.inner, labelsOnFirstNode)
                .updated(quantifiedPathPattern.rightBinding.inner, labelsOnJunctionNode)
            val firstIterationCardinality =
              getPatternRelationshipsCardinality(
                context,
                firstIterationLabels,
                quantifiedPathPattern.patternRelationships
              )

            val intermediateIterationLabels =
              predicates.allLabelInfo
                .updated(quantifiedPathPattern.leftBinding.inner, labelsOnJunctionNode)
                .updated(quantifiedPathPattern.rightBinding.inner, labelsOnJunctionNode)
            val intermediateIterationCardinality =
              getPatternRelationshipsCardinality(
                context,
                intermediateIterationLabels,
                quantifiedPathPattern.patternRelationships
              )
            val intermediateIterationMultiplier =
              Multiplier.ofDivision(intermediateIterationCardinality, junctionNodeCardinality)
                .getOrElse(Multiplier.ZERO)

            val lastIterationLabels =
              predicates.allLabelInfo
                .updated(quantifiedPathPattern.leftBinding.inner, labelsOnJunctionNode)
                .updated(quantifiedPathPattern.rightBinding.inner, labelsOnLastNode)
            val lastIterationCardinality =
              getPatternRelationshipsCardinality(
                context,
                lastIterationLabels,
                quantifiedPathPattern.patternRelationships
              )
            val lastIterationMultiplier =
              Multiplier.ofDivision(lastIterationCardinality, junctionNodeCardinality)
                .getOrElse(Multiplier.ZERO)

            val uniqueRelationshipsInPattern =
              uniqueRelationships.intersect(quantifiedPathPattern.relationshipVariableGroupings.map(_.group.name))
            val uniquenessSelectivity =
              RepetitionCardinalityModel.relationshipUniquenessSelectivity(
                differentRelationships = predicates.differentRelationships.size,
                uniqueRelationships = uniqueRelationshipsInPattern.size,
                repetitions = i
              )

            firstIterationCardinality *
              (intermediateIterationMultiplier ^ (i - 2)) *
              lastIterationMultiplier *
              uniquenessSelectivity *
              (otherPredicatesSelectivity ^ i)
        }.sum(NumericCardinality)

    patternCardinality
  }

  private def getPatternRelationshipsCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    patternRelationships: NonEmptyList[PatternRelationship]
  ): Cardinality = {
    val firstRelationship = patternRelationships.head
    val firstRelationshipCardinality = getSimpleRelationshipCardinality(
      context = context,
      labelInfo = labelInfo,
      leftNode = firstRelationship.left.name,
      rightNode = firstRelationship.right.name,
      relationshipTypes = firstRelationship.types,
      relationshipDirection = firstRelationship.dir
    )
    val otherRelationshipsMultiplier = patternRelationships.tail.view.map { relationship =>
      Multiplier.ofDivision(
        dividend = getSimpleRelationshipCardinality(
          context = context,
          labelInfo = labelInfo,
          leftNode = relationship.left.name,
          rightNode = relationship.right.name,
          relationshipTypes = relationship.types,
          relationshipDirection = relationship.dir
        ),
        divisor = getNodeCardinality(context, labelInfo, relationship.left.name).getOrElse(Cardinality.EMPTY)
      ).getOrElse(Multiplier.ZERO)
    }.product(NumericMultiplier)
    firstRelationshipCardinality * otherRelationshipsMultiplier
  }
}

case class QuantifiedPathPatternPredicates(
  allLabelInfo: LabelInfo,
  differentRelationships: Set[DifferentRelationships],
  otherPredicates: Set[Predicate]
) {
  def labelsOnNode(nodeName: String): Set[LabelName] = allLabelInfo.getOrElse(varFor(nodeName), Set.empty)

  def labelsOnNodes(nodeNames: String*): Set[LabelName] = nodeNames.foldLeft(Set.empty[LabelName]) {
    case (labels, nodeName) => labels.union(labelsOnNode(nodeName))
  }
}

object QuantifiedPathPatternPredicates {

  def partitionSelections(labelInfo: LabelInfo, selections: Selections): QuantifiedPathPatternPredicates = {
    val labelsBuilder = mutable.Map.empty[LogicalVariable, mutable.Builder[LabelName, Set[LabelName]]]
    val differentRelationshipsBuilder = Set.newBuilder[DifferentRelationships]
    val otherPredicatesBuilder = Set.newBuilder[Predicate]
    selections.predicates.foreach {
      case Predicate(_, HasLabels(v: Variable, labels)) =>
        labelsBuilder.updateWith(v)(builder => Some(builder.getOrElse(Set.newBuilder).addOne(labels.head)))
      case Predicate(_, differentRelationships: DifferentRelationships) =>
        differentRelationshipsBuilder.addOne(differentRelationships)
      case otherPredicate =>
        otherPredicatesBuilder.addOne(otherPredicate)
    }
    labelInfo.foreach {
      case (name, labels) =>
        labelsBuilder.updateWith(name)(builder => Some(builder.getOrElse(Set.newBuilder).addAll(labels)))
    }
    QuantifiedPathPatternPredicates(
      allLabelInfo = labelsBuilder.view.mapValues(_.result()).toMap,
      differentRelationships = differentRelationshipsBuilder.result(),
      otherPredicates = otherPredicatesBuilder.result()
    )
  }
}
