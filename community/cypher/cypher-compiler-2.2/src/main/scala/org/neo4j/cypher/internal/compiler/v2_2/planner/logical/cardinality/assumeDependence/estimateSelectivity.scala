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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeDependence

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Multiplier, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId, RelTypeId, functions}
import org.neo4j.graphdb.Direction

case class estimateSelectivity(stats: GraphStatistics, semanticTable: SemanticTable) extends (PredicateCombination => Selectivity) {
  def apply(predicateCombo: PredicateCombination): Selectivity = predicateCombo match {

    // WHERE x:Label
    case SingleExpression(HasLabels(_, label :: Nil)) =>
      calculateSelectivityForLabel(label)

    // MATCH (a:A)-[:T1|T2]->(b:B) - labels and types are optional
    case rel: RelationshipWithLabels =>
      calculateSelectivityForPatterns(rel)

    // If the property name is not known the the schema, nothing will match
    case property: PropertyEqualsAndLabelPredicate if property.propertyKey.propertyKeyId.isEmpty =>
      0.0

    // WHERE x:Label AND x.prop = 42
    case property: PropertyEqualsAndLabelPredicate =>
      val idxLookup: Option[Selectivity] = getSelectivityForPossibleIndex(property)
      idxLookup.map(_ * calculateSelectivityForLabel(property.label)).getOrElse(1.0)

    // WHERE x:Label AND x.prop <> 42
    case property: PropertyNotEqualsAndLabelPredicate =>
      val idxLookup: Option[Selectivity] = getSelectivityForPossibleIndex(property)
      idxLookup.map(_.negate * calculateSelectivityForLabel(property.label)).getOrElse(1.0)

    // WHERE false
    case SingleExpression(False()) =>
      0.0

    // WHERE id(x) = {param}
    case SingleExpression(In(func@FunctionInvocation(_, _, IndexedSeq(_)), Parameter(_)))
      if func.function == Some(functions.Id) =>
      25.0 / stats.nodesWithLabelCardinality(None)

    // WHERE id(x) IN [...]
    case SingleExpression(In(func@FunctionInvocation(_, _, IndexedSeq(_)), c:Collection))
      if func.function == Some(functions.Id) =>
      c.expressions.size / stats.nodesWithLabelCardinality(None)

    case OrCombination(_, inner) =>
      apply(inner)

    case ExistsCombination(_) =>
      1.0

    // WHERE n.prop = <exp>
    case SingleExpression(In(Property(identifier: Identifier, _), Collection(elements))) =>
      0.1 * elements.size

    case _ =>
      GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY
  }

  private def calculateSelectivityForLabel(label: LabelName): Selectivity =
    calculateSelectivityForLabel(label.labelId)

  private def calculateSelectivityForLabel(label: Option[LabelId]): Selectivity = {
    val nodeCardinality: Cardinality = stats.nodesWithLabelCardinality(None)
    if (nodeCardinality == Cardinality(0.0)) {
      return 1.0
    }

    val labelCardinality: Cardinality = label.map(l => stats.nodesWithLabelCardinality(Some(l))).getOrElse(0.0)
    labelCardinality / nodeCardinality
  }

  private def getSelectivityForPossibleIndex(in: PropertyAndLabelPredicate): Option[Selectivity] = {
    val selectivity: Option[Selectivity] = (in.label.labelId, in.propertyKey.propertyKeyId) match {
      case (Some(labelId), Some(propertyKeyId)) =>
        stats.indexSelectivity(labelId, propertyKeyId)

      case _ =>
        Some(0.0)
    }
    selectivity.map(_ * Multiplier(in.valueCount))
  }

  private def calculateSelectivityForPatterns(in: RelationshipWithLabels): Selectivity = in match {
    case RelationshipWithLabels(Some(lhs), pattern, Some(rhs), _) =>
      val maxRelCount:Cardinality = stats.nodesWithLabelCardinality(lhs.labelId) * stats.nodesWithLabelCardinality(rhs.labelId)
      if (maxRelCount == Cardinality(0))
        return 1.0

      val lhsSelectivity = calculateSelectivityForLabel(lhs)
      val rhsSelectivity = calculateSelectivityForLabel(rhs)

      val relCount: Cardinality =
        if (pattern.types.isEmpty) {
          (lhs.labelId, rhs.labelId) match {
            case (Some(lId), Some(rId)) if pattern.dir == Direction.OUTGOING =>
              stats.cardinalityByLabelsAndRelationshipType(Some(lId), None, Some(rId)) * lhsSelectivity * rhsSelectivity
            case (Some(lId), Some(rId)) if pattern.dir == Direction.INCOMING =>
              stats.cardinalityByLabelsAndRelationshipType(Some(rId), None, Some(lId)) * lhsSelectivity * rhsSelectivity
            case (Some(lId), Some(rId)) if pattern.dir == Direction.BOTH =>
              stats.cardinalityByLabelsAndRelationshipType(Some(lId), None, Some(rId)) * lhsSelectivity * rhsSelectivity +
                stats.cardinalityByLabelsAndRelationshipType(Some(rId), None, Some(lId)) * lhsSelectivity * rhsSelectivity
            case _ =>
              0.0
          }

        } else {
          val relationshipId: Option[RelTypeId] = pattern.types.map(_.relTypeId).head
          (lhs.labelId, relationshipId, rhs.labelId) match {
            case (Some(lId), Some(relId), Some(rId)) if pattern.dir == Direction.OUTGOING =>
              stats.cardinalityByLabelsAndRelationshipType(Some(lId), Some(relId), Some(rId)) * lhsSelectivity * rhsSelectivity
            case (Some(lId), Some(relId), Some(rId)) if pattern.dir == Direction.INCOMING =>
              stats.cardinalityByLabelsAndRelationshipType(Some(rId), Some(relId), Some(lId)) * lhsSelectivity * rhsSelectivity
            case (Some(lId), Some(relId), Some(rId)) if pattern.dir == Direction.BOTH =>
              stats.cardinalityByLabelsAndRelationshipType(Some(lId), Some(relId), Some(rId)) * lhsSelectivity * rhsSelectivity +
                stats.cardinalityByLabelsAndRelationshipType(Some(rId), Some(relId), Some(lId)) * lhsSelectivity * rhsSelectivity
            case _ =>
              0.0
          }
        }

      relCount / maxRelCount

    case RelationshipWithLabels( Some( lhs ), pattern, None, _ ) =>
      selectivityForPatternWithLabelsOnOneSide( pattern, pattern.dir, lhs.labelId )

    case RelationshipWithLabels(None, pattern, Some(rhs), _) =>
      selectivityForPatternWithLabelsOnOneSide(pattern, pattern.dir.reverse(), rhs.labelId)

    // No relationship type or labels specified
    case RelationshipWithLabels(None, pattern: PatternRelationship, None, _) if pattern.types.isEmpty =>
      val maxRelCount = stats.nodesWithLabelCardinality(None) ^ 2
      if (maxRelCount == Cardinality(0))
        return 1.0
      val relCount = stats.cardinalityByLabelsAndRelationshipType(None, None, None)
      relCount / maxRelCount

    case RelationshipWithLabels(None, pattern: PatternRelationship, None, _) =>
      val relationshipId: Option[RelTypeId] = pattern.types.map(_.relTypeId).head

      val maxRelCount = stats.nodesWithLabelCardinality(None) ^ 2
      if (maxRelCount == Cardinality(0))
        return 1.0
      val relCount: Cardinality = relationshipId.
        map(id => stats.cardinalityByLabelsAndRelationshipType(None, Some(id), None)).
        getOrElse(0.0)
      relCount / maxRelCount
  }

  private def selectivityForPatternWithLabelsOnOneSide(pattern: PatternRelationship,
                                                       dir: Direction,
                                                       lhsLabelId: Option[LabelId]): Selectivity = {
    val maxRelCount = stats.nodesWithLabelCardinality(None) * stats.nodesWithLabelCardinality(lhsLabelId)
    if (maxRelCount == Cardinality(0))
      return 1.0

    val relCount: Cardinality = (if (pattern.types.isEmpty) {
      lhsLabelId.map { _ =>
        if (dir == Direction.OUTGOING)
          stats.cardinalityByLabelsAndRelationshipType(lhsLabelId, None, None)
        else if (dir == Direction.INCOMING)
          stats.cardinalityByLabelsAndRelationshipType(None, None, lhsLabelId)
        else
          stats.cardinalityByLabelsAndRelationshipType(lhsLabelId, None, None) +
            stats.cardinalityByLabelsAndRelationshipType(None, None, lhsLabelId)
      }
    } else {
      val relationshipId: Option[RelTypeId] = pattern.types.map(_.relTypeId).head
      lhsLabelId.flatMap { _ =>
        relationshipId.map { _ =>
          if (dir == Direction.OUTGOING)
            stats.cardinalityByLabelsAndRelationshipType(lhsLabelId, relationshipId, None)
          else if (dir == Direction.INCOMING)
            stats.cardinalityByLabelsAndRelationshipType(None, relationshipId, lhsLabelId)
          else
            stats.cardinalityByLabelsAndRelationshipType(lhsLabelId, relationshipId, None) +
              stats.cardinalityByLabelsAndRelationshipType(None, relationshipId, lhsLabelId)
        }
      }
    }).getOrElse(0.0)

    // We need to factor in the selectivity of the label predicate as well as the relationship selectivity
    calculateSelectivityForLabel(lhsLabelId) * (relCount / maxRelCount)
  }

  implicit class RichLabelName(val label: LabelName) {
    def labelId: Option[LabelId] = semanticTable.resolvedLabelIds.get(label.name)
  }

  implicit class RichRelTypeName(val relType: RelTypeName) {
    def relTypeId: Option[RelTypeId] = semanticTable.resolvedRelTypeNames.get(relType.name)
  }

  implicit class RichPropertyKeyName(val property: PropertyKeyName) {
    def propertyKeyId: Option[PropertyKeyId] = semanticTable.resolvedPropertyKeyNames.get(property.name)
  }
}
