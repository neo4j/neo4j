/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, PropertyKeyId, LabelId}
import org.neo4j.graphdb._
import collection.JavaConverters._
import scala.collection.mutable

class QueriedGraphStatistics(graph: GraphDatabaseService, queryContext: QueryContext) extends GraphStatistics {

  def nodesWithLabelCardinality(optLabelId: Option[LabelId]): Cardinality =
    Cardinality(optLabelId.map {
      labelId =>
        queryContext.getNodesByLabel(labelId.id)
    }.getOrElse(queryContext.nodeOps.all).size)

  def cardinalityByLabelsAndRelationshipType(optFromLabel: Option[LabelId], optRelTypeId: Option[RelTypeId], optToLabel: Option[LabelId]): Cardinality =
    Cardinality((optFromLabel, optRelTypeId, optToLabel) match {

      case (Some(LabelId(fromLabelId)), Some(RelTypeId(relTypeId)), Some(LabelId(toLabelId))) =>
        val relTypeName = queryContext.getRelTypeName(relTypeId)
        val relType = DynamicRelationshipType.withName(relTypeName)

        val toLabelName = queryContext.getLabelName(toLabelId)

        val s1 = queryContext.getNodesByLabel(fromLabelId)
        val outRels = s1.flatMap { _.getRelationships(relType, Direction.OUTGOING).asScala }
        val result = outRels.filter { (rel: Relationship) =>
          rel.getEndNode.getLabels.asScala.exists(_.name() == toLabelName)
        }
        result.size

      case (Some(LabelId(fromLabelId)), None, Some(LabelId(toLabelId))) =>
        val toLabelName = queryContext.getLabelName(toLabelId)

        val outRels = queryContext.getNodesByLabel(fromLabelId).flatMap { _.getRelationships(Direction.OUTGOING).asScala }
        val result = outRels.filter { (rel: Relationship) =>
          rel.getEndNode.getLabels.asScala.exists(_.name() == toLabelName)
        }
        result.size

      case (None, Some(RelTypeId(relTypeId)), Some(LabelId(toLabelId))) =>
        val relTypeName = queryContext.getRelTypeName(relTypeId)
        val relType = DynamicRelationshipType.withName(relTypeName)
        queryContext.getNodesByLabel(toLabelId).flatMap { _.getRelationships(relType, Direction.INCOMING).asScala }.size

      case (None, None, Some(LabelId(toLabelId))) =>
        queryContext.getNodesByLabel(toLabelId).flatMap { _.getRelationships(Direction.INCOMING).asScala }.size

      case (Some(LabelId(fromLabelId)), Some(RelTypeId(relTypeId)), None) =>
        val relTypeName = queryContext.getRelTypeName(relTypeId)
        val relType = DynamicRelationshipType.withName(relTypeName)
        queryContext.getNodesByLabel(fromLabelId).flatMap { _.getRelationships(relType, Direction.OUTGOING).asScala }.size

      case (Some(LabelId(fromLabelId)), None, None) =>
        queryContext.getNodesByLabel(fromLabelId).flatMap { _.getRelationships(Direction.OUTGOING).asScala }.size

      case (None, Some(RelTypeId(relTypeId)), None) =>
        val relTypeName = queryContext.getRelTypeName(relTypeId)
        queryContext.relationshipOps.all.count(_.getType.name() == relTypeName)

      case (None, None, None) =>
        queryContext.relationshipOps.all.size
    })

  def indexSelectivity(labelId: LabelId, propertyKeyId: PropertyKeyId): Option[Selectivity] = {
    val labelName = queryContext.getLabelName(labelId.id)
    val propertyKeyName = queryContext.getPropertyKeyName(propertyKeyId.id)

    if (!indexExistsOnLabelAndProp(labelName, propertyKeyName))
      None
    else {
      val indexedNodes = queryContext.getNodesByLabel(labelId.id).filter {
        _.hasProperty(propertyKeyName)
      }

      val values = new mutable.HashSet[Any]()
      while (indexedNodes.hasNext) {
        val propertyValue = indexedNodes.next().getProperty(propertyKeyName)
        values += propertyValue
      }

      if (values.isEmpty)
        Some(Selectivity.ZERO) // Avoids division by zero
      else
        Selectivity.of(1.0 / values.size)
    }
  }

  private def indexExistsOnLabelAndProp(labelName: String, propertyKeyName: String) =
    graph.schema().getIndexes(DynamicLabel.label(labelName)).asScala.exists(_.getPropertyKeys.asScala.toSeq == Seq(propertyKeyName))
}
