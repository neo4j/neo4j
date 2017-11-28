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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.planner.v3_4.spi.{GraphStatistics, IndexDescriptor}
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics => GraphStatisticsV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.{IndexDescriptor => IndexDescriptorV3_3}
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.ir.v3_3.{Selectivity => SelectivityV3_3}
import org.neo4j.cypher.internal.frontend.{v3_3 => frontendV3_3}

/**
  * Bridge class to map a 3.3 graph statistics object to a 3.4 one
  */
case class GraphStatisticsWrapper(graphStats: GraphStatisticsV3_3) extends GraphStatistics {
  /**
    * Gets the Cardinality for given LabelId
    *
    * Attention: This method does NOT return the number of nodes anymore!
    *
    * @param labelId Either some labelId for which the Cardinality should be retrieved or None
    * @return returns the Cardinality for the given LabelId or Cardinality(1) for a non-existing label
    */
  override def nodesWithLabelCardinality(labelId: Option[LabelId]) = {
    val cardinalityV3_3 = graphStats.nodesWithLabelCardinality(labelId.map(labelId4To3 _))
    helpers.as3_4(cardinalityV3_3)
  }


  override def nodesAllCardinality() = ??? // TODO: Implement once we rely on 3.3.1 that has this method

  override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]) = {
    val fromLabelV3_3 =  fromLabel.map(labelId4To3 _)
    val relTypeIdV3_3 = relTypeId.map(relTypeId4To3 _)
    val toLabelV3_3 = toLabel.map(labelId4To3 _)
    val cardinalityV3_3 = graphStats.cardinalityByLabelsAndRelationshipType(fromLabelV3_3, relTypeIdV3_3, toLabelV3_3)
    helpers.as3_4(cardinalityV3_3)
  }

  override def indexSelectivity(index: IndexDescriptor) = {
    val indexV3_3 = indexDescriptor4To3(index)
    graphStats.indexSelectivity(indexV3_3).map(selectivity3To4 _)
  }

  override def indexPropertyExistsSelectivity(index: IndexDescriptor) = {
    val indexV3_3 = indexDescriptor4To3(index)
    graphStats.indexPropertyExistsSelectivity(indexV3_3).map(selectivity3To4 _)
  }

  private def labelId4To3(labelId: LabelId) : frontendV3_3.LabelId = {
    frontendV3_3.LabelId(labelId.id)
  }

  private def relTypeId4To3(relTypeId: RelTypeId) : frontendV3_3.RelTypeId = {
    frontendV3_3.RelTypeId(relTypeId.id)
  }

  private def propertyKeyId4To3(propertyKeyId: PropertyKeyId) : frontendV3_3.PropertyKeyId = {
    frontendV3_3.PropertyKeyId(propertyKeyId.id)
  }

  private def indexDescriptor4To3(index: IndexDescriptor) : IndexDescriptorV3_3 = {
    IndexDescriptorV3_3(labelId4To3(index.label),
      index.properties.map(propertyKeyId4To3 _))
  }

  private def selectivity3To4(selectivity: SelectivityV3_3) : Selectivity = {
    Selectivity(selectivity.factor)
  }
}
