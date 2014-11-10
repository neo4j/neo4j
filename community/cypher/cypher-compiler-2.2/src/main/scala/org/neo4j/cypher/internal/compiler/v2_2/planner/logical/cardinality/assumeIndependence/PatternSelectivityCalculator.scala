/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Selections, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.{InternalException, LabelId, NameId, RelTypeId}
import org.neo4j.graphdb.Direction

trait Pattern2Selectivity {
  def apply(pattern: PatternRelationship, labels: Map[IdName, Seq[LabelName]])(implicit semanticTable: SemanticTable, selections: Selections): Selectivity
}

case class PatternSelectivityCalculator(stats: GraphStatistics, combiner: SelectivityCombiner) extends Pattern2Selectivity {

  def apply(pattern: PatternRelationship, labels: Map[IdName, Seq[LabelName]])(implicit semanticTable: SemanticTable, selections: Selections): Selectivity = {
    val lhs = pattern.nodes._1
    val rhs = pattern.nodes._2
    val labelsOnLhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(lhs).toSeq ++ labels.getOrElse(lhs, Seq.empty))
    val labelsOnRhs: Seq[TokenSpec[LabelId]] = mapToLabelTokenSpecs(selections.labelsOnNode(rhs).toSeq ++ labels.getOrElse(rhs, Seq.empty))

    val lhsCardinality = stats.nodesWithLabelCardinality(None) * calculateLabelSelectivity(
      mapToLabelTokenSpecs(selections.labelsOnNode(lhs).toSeq))
    val rhsCardinality = stats.nodesWithLabelCardinality(None) * calculateLabelSelectivity(
      mapToLabelTokenSpecs(selections.labelsOnNode(rhs).toSeq))
    val maxRelCount = lhsCardinality * rhsCardinality
    val types = mapToRelTokenSpecs(pattern.types)

    if (maxRelCount == Cardinality(0))
      Selectivity(1)
    else
      calculateRelSelectivity(types, labelsOnLhs, labelsOnRhs, pattern.dir, maxRelCount)
  }

  private def calculateRelSelectivity(types: Seq[TokenSpec[RelTypeId]],
                                      labelsOnLhs: Seq[TokenSpec[LabelId]],
                                      labelsOnRhs: Seq[TokenSpec[LabelId]],
                                      dir: Direction,
                                      maxRelCount: Cardinality): Selectivity = {
    val selectivities = (types map { typ =>
      for {
        lhsLabel <- labelsOnLhs
        rhsLabel <- labelsOnRhs
      } yield {
        (lhsLabel, typ, rhsLabel) match {
          //          If the rel-type or either label are unknown to the schema, we know no matches will be had
          case (SpecifiedButUnknown(), _, _) | (_, SpecifiedButUnknown(), _) | (_, _, SpecifiedButUnknown()) =>
            Selectivity(0)

          case _ if dir == Direction.OUTGOING =>
            stats.cardinalityByLabelsAndRelationshipType(lhsLabel.id, typ.id, rhsLabel.id) / maxRelCount

          case _ if dir == Direction.INCOMING =>
            stats.cardinalityByLabelsAndRelationshipType(rhsLabel.id, typ.id, lhsLabel.id) / maxRelCount

          case _ if dir == Direction.BOTH =>
            combiner.orTogetherSelectivities(Seq(
              stats.cardinalityByLabelsAndRelationshipType(lhsLabel.id, typ.id, rhsLabel.id) / maxRelCount,
              stats.cardinalityByLabelsAndRelationshipType(rhsLabel.id, typ.id, lhsLabel.id) / maxRelCount
            )).get
        }
      }
    }).map(combiner.andTogetherSelectivities).flatten

    combiner.orTogetherSelectivities(selectivities).getOrElse(Selectivity(1))
  }

  private def calculateLabelSelectivity(specs: Seq[TokenSpec[LabelId]]): Selectivity = {
    val selectivities = specs map {
      case SpecifiedButUnknown() => Selectivity(0)
      case spec: TokenSpec[LabelId] => stats.nodesWithLabelCardinality(spec.id) / stats.nodesWithLabelCardinality(None)
    }

    combiner.andTogetherSelectivities(selectivities).getOrElse(Selectivity(1))
  }

  // These two methods should be one, but I failed to conjure up the proper Scala type magic to make it work
  private def mapToLabelTokenSpecs(input: Seq[LabelName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[LabelId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.map {
        case label =>
          label.
            id.
            map(SpecifiedAndKnown.apply).
            getOrElse(SpecifiedButUnknown())
      }


  private def mapToRelTokenSpecs(input: Seq[RelTypeName])(implicit semanticTable: SemanticTable): Seq[TokenSpec[RelTypeId]] =
    if (input.isEmpty)
      Seq(Unspecified())
    else
      input.map {
        case rel =>
          rel.
            id.
            map(SpecifiedAndKnown.apply).
            getOrElse(SpecifiedButUnknown())
      }
}

sealed trait TokenSpec[+ID <: NameId] {
  def id: Option[ID]
}

case class SpecifiedButUnknown() extends TokenSpec[Nothing] {
  def id = throw new InternalException("Tried to use a token id unknown to the schema")
}

case class Unspecified() extends TokenSpec[Nothing] {
  def id = None
}

case class SpecifiedAndKnown[+ID <: NameId](_id: ID) extends TokenSpec[ID] {
  def id = Some(_id)
}
