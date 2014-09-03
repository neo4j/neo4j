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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.v2_2.ast.{PropertyKeyName, HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Multiplier, Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, TokenContext}
import org.neo4j.cypher.internal.compiler.v2_2.{PropertyKeyId, LabelId, RelTypeId}
import org.neo4j.graphdb.Direction

case class estimateSelectivity(stats: GraphStatistics, tokens: TokenContext) extends (PredicateCombination => Selectivity) {
  def apply(predicateCombo: PredicateCombination): Selectivity = predicateCombo match {
    case SingleExpression(HasLabels(_, label :: Nil)) =>
      calculateSelectivityForLabel(label)

    case rel: RelationshipWithLabels =>
      calculateSelectivityForPatterns(rel)

    case property: PropertyEqualsAndLabelPredicate =>
      calculateSelectivityForLabel(property.label) * calculateSelectivityForPropertyLookup(property)

    case property: PropertyNotEqualsAndLabelPredicate =>
      calculateSelectivityForLabel(property.label) * (Selectivity(1) - calculateSelectivityForPropertyLookup(property))

    case _ =>
      GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY
  }

  private def calculateSelectivityForLabel(label: LabelName): Selectivity = {
    val nodeCardinality = stats.nodesWithLabelCardinality(None)
    if (nodeCardinality == Cardinality(0)) {
      return Selectivity(1)
    }

    val maybeLabelId: Option[Int] = tokens.getOptLabelId(label.name)
    val labelCardinality = maybeLabelId.
      map(l => stats.nodesWithLabelCardinality(Some(LabelId(l)))).
      getOrElse(Cardinality(0))
    labelCardinality / nodeCardinality
  }

  private def calculateSelectivityForPropertyLookup(in: PropertyAndLabelPredicate): Selectivity = {
    ((in.label.labelId, in.propertyKey.propertyKeyId) match {
      case (Some(labelId), Some(propertyKeyId)) =>
        stats.indexSelectivity(labelId, propertyKeyId).getOrElse(Selectivity(1))

      case _ =>
        Selectivity(0)
    }) * Multiplier(in.valueCount)
  }

  private def calculateSelectivityForPatterns(in: RelationshipWithLabels): Selectivity = {
    in match {
      case RelationshipWithLabels(Some(lhs), pattern, Some(rhs), _) =>
        val relationshipId: Option[Int] = tokens.getOptRelTypeId(pattern.types.head.name)
        val maxRelCount = stats.nodesWithLabelCardinality(lhs.labelId) * stats.nodesWithLabelCardinality(rhs.labelId)
        if (maxRelCount == Cardinality(0))
          return Selectivity(1)

        val relCount = (lhs.labelId, relationshipId, rhs.labelId) match {
          case (Some(lId), Some(relId), Some(rId)) =>
            stats.cardinalityByLabelsAndRelationshipType(Some(lId), Some(RelTypeId(relId)), Some(rId)) *
              calculateSelectivityForLabel(lhs) *
              calculateSelectivityForLabel(rhs)
          case _ =>
            Cardinality(0)
        }
        relCount / maxRelCount

      case RelationshipWithLabels(Some(lhs), pattern, None, _) =>
        selectivityForPatternWithLabelsOnOneSide(pattern, pattern.dir, lhs.labelId) *
          calculateSelectivityForLabel(lhs)

      case RelationshipWithLabels(None, pattern, Some(rhs), _) =>
        selectivityForPatternWithLabelsOnOneSide(pattern, pattern.dir.reverse(), rhs.labelId) *
          calculateSelectivityForLabel(rhs)

      case RelationshipWithLabels(None, pattern, None, _) =>
        val relationshipId: Option[Int] = tokens.getOptRelTypeId(pattern.types.head.name)

        val maxRelCount = stats.nodesWithLabelCardinality(None) ^ 2
        if (maxRelCount == Cardinality(0))
          return Selectivity(1)
        val relCount = relationshipId.
          map(id => stats.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(id)), None)).
          getOrElse(Cardinality(0))
        relCount / maxRelCount
    }
  }

  private def selectivityForPatternWithLabelsOnOneSide(pattern: PatternRelationship,
                                                       dir: Direction,
                                                       lhsLabelId: Option[LabelId]): Selectivity = {
    val relationshipId: Option[Int] = tokens.getOptRelTypeId(pattern.types.head.name)
    val maxRelCount = stats.nodesWithLabelCardinality(None) * stats.nodesWithLabelCardinality(lhsLabelId)
    if (maxRelCount == Cardinality(0))
      return Selectivity(1)

    val relCount = (lhsLabelId, relationshipId) match {
      case (Some(lId), Some(relId)) if dir == Direction.OUTGOING =>
        stats.cardinalityByLabelsAndRelationshipType(Some(lId), Some(RelTypeId(relId)), None)
      case (Some(lId), Some(relId)) if dir == Direction.INCOMING =>
        stats.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(relId)), Some(lId))
      case _ =>
        Cardinality(0)
    }
    relCount / maxRelCount
  }

  implicit class RichLabelName(val label: LabelName) {
    def labelId = tokens.getOptLabelId(label.name).map(LabelId)
  }
  implicit class RichPropertyKeyName(val property: PropertyKeyName) {
    def propertyKeyId = tokens.getOptPropertyKeyId(property.name).map(PropertyKeyId)
  }
}
