/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_2.ast.{LabelName, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Selections, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.{InternalException, LabelId, RelTypeId}
import org.neo4j.graphdb.Direction

trait Pattern2Selectivity {
  def apply(pattern: PatternRelationship, labels: Map[IdName, Set[LabelName]])(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

object PatternSelectivityCalculator {
  val MAX_VAR_LENGTH = 32
}

case class PatternSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Pattern2Selectivity {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.PatternSelectivityCalculator._

  def apply(pattern: PatternRelationship, labels: Map[IdName, Set[LabelName]])
           (implicit semanticTable: SemanticTable, selections: Selections): Selectivity = {
    val allNodes = stats.nodesWithLabelCardinality(None)
    val lhs = pattern.nodes._1
    val rhs = pattern.nodes._2
    val labelsOnLhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(lhs) ++ labels.getOrElse(lhs, Set.empty))
    val labelsOnRhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(rhs) ++ labels.getOrElse(rhs, Set.empty))

    val lhsCardinality = allNodes * calculateLabelSelectivity(labelsOnLhs)
    val rhsCardinality = allNodes * calculateLabelSelectivity(labelsOnRhs)

    // If either side of our pattern is empty, it's all empty
    if (lhsCardinality == Cardinality.EMPTY || lhsCardinality == Cardinality.EMPTY)
      Selectivity(1)
    else {
      val types: Seq[TokenSpec[RelTypeId]] = mapToRelTokenSpecs(pattern.types.toSet)

      pattern.length match {
        case SimplePatternLength =>
          calculateSelectivityForSingleRelHop(types, labelsOnLhs, labelsOnRhs, pattern.dir, lhsCardinality * rhsCardinality)

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
          val maxRelCount = lhsCardinality * rhsCardinality * Cardinality(Math.pow(allNodes.amount, max - 1))

          val selectivityPerLengthAndStep: Seq[Seq[Selectivity]] =
            for (length <- min to max)
              yield {
                for (i <- 1 to length)
                  yield {
                    val labelsOnL: Seq[TokenSpec[LabelId]] = if (i == 1) labelsOnLhs else Seq(Unspecified())
                    val labelsOnR: Seq[TokenSpec[LabelId]] = if (i == length) labelsOnRhs else Seq(Unspecified())
                    calculateSelectivityForSingleRelHop(types, labelsOnL, labelsOnR, pattern.dir, maxRelCount)
                  }
              }
          val selectivityPerLength = selectivityPerLengthAndStep.flatMap(combiner.andTogetherSelectivities)
          combiner.orTogetherSelectivities(selectivityPerLength).
            getOrElse(throw new InternalException("Unexpectedly tried to calculate cardinality of a [*0..0] relationship"))
      }
    }
  }

  private def calculateSelectivityForSingleRelHop(types: Seq[TokenSpec[RelTypeId]],
                                                  labelsOnLhs: Seq[TokenSpec[LabelId]],
                                                  labelsOnRhs: Seq[TokenSpec[LabelId]],
                                                  dir: Direction,
                                                  maxRelCount: Cardinality): Selectivity = {

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

          case _ if dir == Direction.OUTGOING =>
            stats.cardinalityByLabelsAndRelationshipType(lhsLabel.id, typ.id, rhsLabel.id) / maxRelCount getOrElse Selectivity.ONE

          case _ if dir == Direction.INCOMING =>
            stats.cardinalityByLabelsAndRelationshipType(rhsLabel.id, typ.id, lhsLabel.id) / maxRelCount getOrElse Selectivity.ONE

          case _ if dir == Direction.BOTH =>
            combiner.orTogetherSelectivities(
              Seq(
                (stats.cardinalityByLabelsAndRelationshipType(lhsLabel.id, typ.id, rhsLabel.id) / maxRelCount) getOrElse Selectivity.ONE,
                (stats.cardinalityByLabelsAndRelationshipType(rhsLabel.id, typ.id, lhsLabel.id) / maxRelCount) getOrElse Selectivity.ONE
              )).get
        }
      }
    }

    val selectivitiesPerType = selectivitiesPerTypeAndLabel.flatMap(combiner.andTogetherSelectivities)
    val combinedSelectivity = combiner.orTogetherSelectivities(selectivitiesPerType).get
    combinedSelectivity
  }

  private def calculateLabelSelectivity(specs: Seq[TokenSpec[LabelId]]): Selectivity = {
    val selectivities = specs map {
      case SpecifiedButUnknown() => Selectivity(0)
      case spec: TokenSpec[LabelId] =>
        stats.nodesWithLabelCardinality(spec.id) / stats.nodesWithLabelCardinality(None) getOrElse Selectivity.ZERO
    }

    combiner.andTogetherSelectivities(selectivities).getOrElse(Selectivity(1))
  }

  // These two methods should be one, but I failed to conjure up the proper Scala type magic to make it work
  private def mapToLabelTokenSpecs(input: Set[LabelName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[LabelId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.toSeq.map {
        case label =>
          label.
            id.
            map(SpecifiedAndKnown.apply).
            getOrElse(SpecifiedButUnknown())
      }


  private def mapToRelTokenSpecs(input: Set[RelTypeName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[RelTypeId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.toSeq.map {
        case rel =>
          rel.
            id.
            map(SpecifiedAndKnown.apply).
            getOrElse(SpecifiedButUnknown())
      }
}
