/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_3.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.ast.{LabelName, RelTypeName}
import org.neo4j.cypher.internal.frontend.v2_3._

trait Pattern2Selectivity {
  def apply(pattern: PatternRelationship, labels: Map[IdName, Set[LabelName]])(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

object PatternSelectivityCalculator {
  val MAX_VAR_LENGTH = 32
}

case class PatternSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Pattern2Selectivity {

  import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.assumeIndependence.PatternSelectivityCalculator._

  def apply(pattern: PatternRelationship, labels: Map[IdName, Set[LabelName]])
           (implicit semanticTable: SemanticTable, selections: Selections): Selectivity = {
    val nbrOfNodesInGraph = stats.nodesWithLabelCardinality(None)
    val (lhs, rhs) = pattern.nodes
    val labelsOnLhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(lhs) ++ labels.getOrElse(lhs, Set.empty))
    val labelsOnRhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(rhs) ++ labels.getOrElse(rhs, Set.empty))

    val lhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnLhs, nbrOfNodesInGraph)
    val rhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnRhs, nbrOfNodesInGraph)

    // If either side of our pattern is empty, it's all empty
    if (lhsCardinality == Cardinality.EMPTY || rhsCardinality == Cardinality.EMPTY)
      Selectivity.ZERO
    else {
      val estimatedCardinalityBasedOnLabels: Cardinality = lhsCardinality * rhsCardinality

      val types: Seq[TokenSpec[RelTypeId]] = mapToRelTokenSpecs(pattern.types.toSet)

      pattern.length match {
        case SimplePatternLength =>
          calculateSelectivityForSingleRelHop(types, labelsOnLhs, labelsOnRhs, pattern.dir, estimatedCardinalityBasedOnLabels)

        case VarPatternLength(suppliedMin, optMax) =>

          val max = Math.min(optMax.getOrElse(MAX_VAR_LENGTH), MAX_VAR_LENGTH)
          val min = Math.min(suppliedMin, max)
          /*
          How to calculate var-length path selectivity can be exemplified like this

          SELECTIVITY( (a:A)-[:T*3..3]->(b:B) ) =

          ( CARDINALITY( (a:A)-[:T]->(b:B) ) +
            CARDINALITY( (a:A)-[:T]->()-[:T]->(b:B) ) +
            CARDINALITY( (a:A)-[:T]->()-[:T]->()-[:T]->(b:B) ) / crossProductOfNodes
           */
          val estimatedMaxCardForPatternBasedOnLabels = estimatedCardinalityBasedOnLabels * Cardinality(Math.pow(nbrOfNodesInGraph.amount, max - 1))

          val selectivityPerLengthAndStep: Seq[Seq[Selectivity]] =
            for (length <- min to max)
              yield {
                for (i <- 1 to length)
                  yield {
                    val labelsOnL: Seq[TokenSpec[LabelId]] = if (i == 1) labelsOnLhs else Seq(Unspecified())
                    val labelsOnR: Seq[TokenSpec[LabelId]] = if (i == length) labelsOnRhs else Seq(Unspecified())
                    calculateSelectivityForSingleRelHop(types, labelsOnL, labelsOnR, pattern.dir, estimatedMaxCardForPatternBasedOnLabels)
                  }
              }
          val selectivityPerLength = selectivityPerLengthAndStep.flatMap(combiner.andTogetherSelectivities)
          combiner.orTogetherSelectivities(selectivityPerLength).
            getOrElse(Selectivity.ZERO)
      }
    }
  }

  private def calculateSelectivityForSingleRelHop(types: Seq[TokenSpec[RelTypeId]],
                                                  labelsOnLhs: Seq[TokenSpec[LabelId]],
                                                  labelsOnRhs: Seq[TokenSpec[LabelId]],
                                                  dir: SemanticDirection,
                                                  estimatedMaxCardForPatternBasedOnLabels: Cardinality): Selectivity = {

    def selectivityEstimateForPattern(fromLabel: Option[LabelId], relType: Option[RelTypeId], toLabel: Option[LabelId]): Selectivity = {
      stats.cardinalityByLabelsAndRelationshipType(fromLabel, relType, toLabel) / estimatedMaxCardForPatternBasedOnLabels getOrElse Selectivity.ONE
    }

    // (a:A:B)-[r:T1|T2]->(c:C:D)    WHERE a:A AND a:B and type(r) = "T1" OR type(r) = "T2"
    val selectivitiesPerTypeAndLabel: Seq[Seq[Selectivity]] = types map { typ =>
      for {
        lhsLabel <- labelsOnLhs
        rhsLabel <- labelsOnRhs
      } yield {
        (lhsLabel, typ, rhsLabel) match {
          // If the rel-type or either label are unknown to the schema, we know no matches will be had
          case (SpecifiedButUnknown(), _, _) | (_, SpecifiedButUnknown(), _) | (_, _, SpecifiedButUnknown()) =>
            Selectivity.ZERO

          case _ if dir == SemanticDirection.OUTGOING =>
            selectivityEstimateForPattern(lhsLabel.id, typ.id, rhsLabel.id)

          case _ if dir == SemanticDirection.INCOMING =>
            selectivityEstimateForPattern(rhsLabel.id, typ.id, lhsLabel.id)

          case _ if dir == SemanticDirection.BOTH =>
            val selectivities = Seq(
              selectivityEstimateForPattern(lhsLabel.id, typ.id, rhsLabel.id),
              selectivityEstimateForPattern(rhsLabel.id, typ.id, lhsLabel.id)
            )
            combiner.orTogetherSelectivities(selectivities).get
        }
      }
    }

    val selectivitiesPerType = selectivitiesPerTypeAndLabel.flatMap(combiner.andTogetherSelectivities)
    val combinedSelectivity = combiner.orTogetherSelectivities(selectivitiesPerType).get
    combinedSelectivity
  }

  private def calculateLabelSelectivity(specs: Seq[TokenSpec[LabelId]], totalNbrOfNodes: Cardinality): Selectivity = {
    val selectivities = specs map {
      case SpecifiedButUnknown() => Selectivity.ZERO
      case spec: TokenSpec[LabelId] =>
        stats.nodesWithLabelCardinality(spec.id) / totalNbrOfNodes getOrElse Selectivity.ZERO
    }

    combiner.andTogetherSelectivities(selectivities).getOrElse(Selectivity.ONE)
  }

  // These two methods should be one, but I failed to conjure up the proper Scala type magic to make it work
  private def mapToLabelTokenSpecs(input: Set[LabelName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[LabelId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.toSeq.map {
        case label =>
          label.id.map(SpecifiedAndKnown.apply).getOrElse(SpecifiedButUnknown())
      }


  private def mapToRelTokenSpecs(input: Set[RelTypeName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[RelTypeId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.toSeq.map {
        case rel =>
          rel.id.map(SpecifiedAndKnown.apply).getOrElse(SpecifiedButUnknown())
      }
}
