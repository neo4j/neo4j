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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SpecifiedAndKnown
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SpecifiedButUnknown
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.TokenSpec
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.Unspecified
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.PatternRelationshipMultiplierCalculator.MAX_VAR_LENGTH
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.PatternRelationshipMultiplierCalculator.uniquenessSelectivityForNRels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

object PatternRelationshipMultiplierCalculator {
  val MAX_VAR_LENGTH = 32

  def uniquenessSelectivityForNRels(n: Int): Selectivity = {
    require(n >= 1, "Cannot calculate relationship uniqueness for less than 1 relationship")
    val numberOfPairs = n * (n - 1) / 2
    Selectivity(Math.pow(DEFAULT_REL_UNIQUENESS_SELECTIVITY.factor, numberOfPairs))
  }
}

/**
 * This class calculates multipliers for pattern relationships.
 * This means, for a pattern (a)-[r]-(b), the Cardinality for the cross product of (a,b) multiplied with the return value of this function is the cardinality of the pattern.
 * Similarly, for a pattern (a)-[r1]-(b)-[r2]-(c) the Cardinality for the cross product of ((a)-[r1]-(b),c) multiplied with the return value of this function is the cardinality of the whole pattern.
 *
 * It returns Multipliers instead of Selectivities, since a relationship pattern can increase the Cardinality of the cross product of the start and end nodes.
 * This is the case if there are on average more than 1 relationship between nodes, and can also be the case for var length relationships.
 */
case class PatternRelationshipMultiplierCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) {

  private implicit val numericCardinality: NumericCardinality.type = NumericCardinality

  private implicit val numericMultiplier: NumericMultiplier.type = NumericMultiplier

  def relationshipMultiplier(pattern: PatternRelationship, labels: LabelInfo)
                            (implicit semanticTable: SemanticTable): Multiplier = {
    val nbrOfNodesInGraph = stats.nodesAllCardinality()
    val (lhs, rhs) = pattern.nodes
    val Seq(labelsOnLhs, labelsOnRhs) = Seq(lhs, rhs).map(side => mapToLabelTokenSpecs(labels.getOrElse(side, Set.empty)))

    val lhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnLhs, nbrOfNodesInGraph)
    val rhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnRhs, nbrOfNodesInGraph)

    // If either side of our pattern is empty, it's all empty
    if (lhsCardinality == Cardinality.EMPTY || rhsCardinality == Cardinality.EMPTY) {
      Multiplier.ZERO
    } else {
      val types: Seq[TokenSpec[RelTypeId]] = mapToRelTokenSpecs(pattern.types.toSet)

      pattern.length match {
        case SimplePatternLength =>
          calculateMultiplierForSingleRelHop(types, labelsOnLhs, labelsOnRhs, pattern.dir, lhsCardinality, rhsCardinality, nbrOfNodesInGraph)

        case VarPatternLength(suppliedMin, optMax) =>
          val max = Math.min(optMax.getOrElse(MAX_VAR_LENGTH), MAX_VAR_LENGTH)
          val min = Math.min(suppliedMin, max)
          /*
           * An example how we calculate var-length path multipliers:
           *
           * MULTIPLIER( (a:A)-[:R*1..2]->(b:B) )
           * =   MULTIPLIER( (a:A)-[:R*1..1]->(b:B) )                    // The multiplier is the sum of the multipliers of all possible lengths of the path
           *   + MULTIPLIER( (a:A)-[:R*2..2]->(b:B) )
           * = CARDINALITY( (:A)-[:R]->(:B) ) / CARDINALITY( (:A),(:B) ) // The multiplier for the length 1 path is equal to the multiplier of a SimplePatternLength relationship
           *   +   CARDINALITY( (:A)-[:R]->() ) / CARDINALITY( (:A),() ) // The multiplier for the length 2 path multiplies SimplePatternLength relationship multipliers for each step
           *     * CARDINALITY( ()-[:R]->(:B) ) / CARDINALITY( (),(:B) )
           *     * DEFAULT_REL_UNIQUENESS_SELECTIVITY                    // It also needs to include uniqueness selectivity across all relationships of the path
           *     * CARDINALITY( () )                                     // Since the base cardinality that the Multiplier is applied to is CARDINALITY( (:A),(:B) ), we need to multiply with the cardinality of the intermediate node as well
           */
          val multipliersPerLength: Seq[Multiplier] =
            for (length <- min to max) yield {
              length match {
                case 0 =>
                  /* 0 length relationships are weird.
                   * MATCH (a:A)-[:R*0..0]->(b:B) is somewhat equivalent to MATCH (a:A:B)
                   * Since the base cardinality that the Multiplier is applied to is CARDINALITY( (:A),(:B) )
                   * and we want the result to be CARDINALITY ( (:A:B) ), we need to divide by CARDINALITY ( () )
                   */
                  Multiplier.ofDivision(1, nbrOfNodesInGraph)
                case _ =>
                  val stepMultipliers = for (i <- 1 to length) yield {
                    val labelsOnL: Seq[TokenSpec[LabelId]] = if (i == 1) labelsOnLhs else Seq(Unspecified)
                    val labelsOnR: Seq[TokenSpec[LabelId]] = if (i == length) labelsOnRhs else Seq(Unspecified)
                    val lhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnL, nbrOfNodesInGraph)
                    val rhsCardinality = nbrOfNodesInGraph * calculateLabelSelectivity(labelsOnR, nbrOfNodesInGraph)
                    val relMultiplier = calculateMultiplierForSingleRelHop(types, labelsOnL, labelsOnR, pattern.dir, lhsCardinality, rhsCardinality, nbrOfNodesInGraph)
                    // Since the base cardinality that the Multiplier is applied to is the cross product of the start and end of the path, we need to multiply with the cardinality of the intermediate nodes as well
                    if (i == length) relMultiplier else relMultiplier * rhsCardinality.amount
                  }
                  // We multiply for each step to get the overall multiplier for the path.
                  // Since the relationship uniqueness is not added as an extra predicate, like for any pair of simple length relationships,
                  // we need to weight the relationship uniqueness in here as well.
                  stepMultipliers.product * uniquenessSelectivityForNRels(length)
              }
            }
          // Different lengths are exclusive, we we can simply add the Multipliers.
          multipliersPerLength.sum
      }
    }
  }

  private def calculateMultiplierForSingleRelHop(types: Seq[TokenSpec[RelTypeId]],
                                                 labelsOnLhs: Seq[TokenSpec[LabelId]],
                                                 labelsOnRhs: Seq[TokenSpec[LabelId]],
                                                 dir: SemanticDirection,
                                                 lhsCardinality: Cardinality,
                                                 rhsCardinality: Cardinality,
                                                 totalNbrOfNodes: Cardinality): Multiplier = {

    val cardinalitiesPerTypeAndLabel: Seq[Seq[Cardinality]] = types map { typ =>
      for {
        lhsLabel <- labelsOnLhs
        rhsLabel <- labelsOnRhs
      } yield {
        // For each combination of label, type, label, get the cardinality from the store
        val cardinalityForOneLabelCombination = (lhsLabel, typ, rhsLabel) match {
          // If the rel-type or either label are unknown to the schema, we know no matches will be had
          case (SpecifiedButUnknown(), _, _) | (_, SpecifiedButUnknown(), _) | (_, _, SpecifiedButUnknown()) =>
            Cardinality.EMPTY

          case _ if dir == SemanticDirection.OUTGOING =>
            stats.patternStepCardinality(lhsLabel.id, typ.id, rhsLabel.id)

          case _ if dir == SemanticDirection.INCOMING =>
            stats.patternStepCardinality(rhsLabel.id, typ.id, lhsLabel.id)

          case _ if dir == SemanticDirection.BOTH =>
            val cardinalities = Seq(
              stats.patternStepCardinality(lhsLabel.id, typ.id, rhsLabel.id),
              stats.patternStepCardinality(rhsLabel.id, typ.id, lhsLabel.id)
            )
            // Relationship directions are exclusive, we we can simply add the Cardinalties.
            cardinalities.sum
        }
        // If there are multiple labels on start and/or end nodes,
        // we need to apply all labels not part of the current combination as selectivities.
        // For example, when looking at the pattern (:A:B)-[:R]->(:C:D), and currently looking at (:A, :R, :C)
        // we need to multiply that cardinality with the selectivities for :B and :D.
        val otherLabelsSelectivity = calculateLabelSelectivity(labelsOnLhs.filterNot(_ == lhsLabel) ++ labelsOnRhs.filterNot(_ == rhsLabel), totalNbrOfNodes)
        cardinalityForOneLabelCombination * otherLabelsSelectivity
      }
    }

    // If there are multiple labels on start and/or end nodes,
    // we want to get the intersection of cardinalities for each label combination.
    // We have no information about this intersection, except that it cannot be larger than the minimum, so we take that.
    // For further considerations, see the comment at the end of this file.
    val cardinalitiesPerType = cardinalitiesPerTypeAndLabel.map(_.min)
    // Relationship types are exclusive, we we can simply add the Cardinalties.
    val combinedCardinality = cardinalitiesPerType.sum

    // To get a multiplier, we divide by the cardinality of the cross product of (lhs,rhs)
    Multiplier.ofDivision(combinedCardinality, lhsCardinality * rhsCardinality)
  }

  private def calculateLabelSelectivity(specs: Seq[TokenSpec[LabelId]], totalNbrOfNodes: Cardinality): Selectivity = {
    val selectivities = specs map {
      case SpecifiedButUnknown() => Selectivity.ZERO
      case Unspecified => Selectivity.ONE
      case SpecifiedAndKnown(spec: LabelId) =>  // Specified labels have ids
        stats.nodesWithLabelCardinality(Some(spec)) / totalNbrOfNodes getOrElse Selectivity.ZERO
    }

    combiner.andTogetherSelectivities(selectivities).getOrElse(Selectivity.ONE)
  }

  // These two methods should be one, but I failed to conjure up the proper Scala type magic to make it work
  private def mapToLabelTokenSpecs(input: Set[LabelName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[LabelId]] =
    if (input.isEmpty)
      Seq(Unspecified)
    else
      input.toIndexedSeq.map(label =>
        semanticTable.id(label).map(SpecifiedAndKnown.apply).getOrElse(SpecifiedButUnknown()))


  private def mapToRelTokenSpecs(input: Set[RelTypeName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[RelTypeId]] =
    if (input.isEmpty)
      Seq(Unspecified)
    else
      input.toIndexedSeq.map(rel =>
        semanticTable.id(rel).map(SpecifiedAndKnown.apply).getOrElse(SpecifiedButUnknown()))
}

/**

 When estimating the cardinality of (:A:B)--(:C). taking the average of multipliers for (:A)--(C) and (:B)--(:C) is an alternative to
 taking the minimum of (:A)--(:C) * sel(:B) and (:B)--(:C) * sel(:A), which is what we currently do.

 Unfortunately, we already overestimate patterns with labels on both sides, like (:A)--(:C), because we do not keep counts for those.
 This overestimation can propagate by using the average instead of the minimum, but it can be kept down when using the minimum.
 Of course there are even with this constraint cases where averaging performs better, but we argue that this is outweighed by the amount
 it overestimates in other cases.

 If we ever should have exact counts for patterns like (:A)--(:C), we should re-evaluate using the average instead.
 The code we used to implement that can be found below, for reference.

  private def calculateMultiplierForSingleRelHop(types: Seq[TokenSpec[RelTypeId]],
                                                 labelsOnLhs: Seq[TokenSpec[LabelId]],
                                                 labelsOnRhs: Seq[TokenSpec[LabelId]],
                                                 dir: SemanticDirection,
                                                 lhsCardinality: Cardinality,
                                                 rhsCardinality: Cardinality,
                                                 totalNbrOfNodes: Cardinality): Multiplier = {

    /**
     * Given a LHS label :A, a rel type :REL and a RHS label :B
     *
     * @param cardinality the cardinality for the (:A)-[:REL]-(:B)
     * @param lhsCardinality the cardinality for (:A)
     * @param rhsCardinality the cardinality for (:B)
     */
    case class CardinalitySpec(cardinality: Cardinality,
                               lhsCardinality: Cardinality,
                               rhsCardinality: Cardinality)

    // Record cardinality specs for all label combinations, for each relationship type.
    val cardinalitySpecsPerTypeAndLabel: Seq[Seq[CardinalitySpec]] = types map { typ =>
      for {
        lhsLabel <- labelsOnLhs
        lhsCardinality = getLabelCardinality(lhsLabel, totalNbrOfNodes)
        rhsLabel <- labelsOnRhs
        rhsCardinality = getLabelCardinality(rhsLabel, totalNbrOfNodes)
      } yield {
        // For each combination of label, type, label, get the cardinality from the store
        val cardinalityForOneLabelCombination = (lhsLabel, typ, rhsLabel) match {
          // If the rel-type or either label are unknown to the schema, we know no matches will be had
          case (SpecifiedButUnknown(), _, _) | (_, SpecifiedButUnknown(), _) | (_, _, SpecifiedButUnknown()) =>
            Cardinality.EMPTY

          case _ if dir == SemanticDirection.OUTGOING =>
            stats.patternStepCardinality(lhsLabel.id, typ.id, rhsLabel.id)

          case _ if dir == SemanticDirection.INCOMING =>
            stats.patternStepCardinality(rhsLabel.id, typ.id, lhsLabel.id)

          case _ if dir == SemanticDirection.BOTH =>
            val cardinalities = Seq(
              stats.patternStepCardinality(lhsLabel.id, typ.id, rhsLabel.id),
              stats.patternStepCardinality(rhsLabel.id, typ.id, lhsLabel.id)
            )
            // Relationship directions are exclusive, we we can simply add the Cardinalities.
            cardinalities.sum
        }
        CardinalitySpec(cardinalityForOneLabelCombination, lhsCardinality, rhsCardinality)
      }
    }

    // For each type, avg over the multipliers that can be derived from the cardinality specs, but apply the known upper bound of each single cardinality.
    val cardinalitiesPerType = cardinalitySpecsPerTypeAndLabel.map { cardinalitySpecs =>
      val multipliers = cardinalitySpecs.map {
        case CardinalitySpec(cardinality, lhsCardinality, rhsCardinality) =>
          Multiplier.ofDivision(cardinality, lhsCardinality * rhsCardinality)
      }
      // Compute the geometric mean
      val avgMultiplier = Multiplier(Math.pow(multipliers.product.coefficient, 1.0 / multipliers.length))
      // To get a Cardinality, multiply by the cardinality of the cross product of (lhs,rhs)
      val avgCardinality = lhsCardinality * rhsCardinality * avgMultiplier
      // We know that the cardinality cannot be bigger than any single cardinality in the CardinalitySpec
      val upperBound = cardinalitySpecs.map(_.cardinality).min
      Cardinality.min(avgCardinality, upperBound)
    }
    // Relationship types are exclusive, we we can simply add the Cardinalities.
    val combinedCardinality = cardinalitiesPerType.sum

    // To get a multiplier, we divide by the cardinality of the cross product of (lhs,rhs)
    Multiplier.ofDivision(combinedCardinality, lhsCardinality * rhsCardinality)
  }
 */
