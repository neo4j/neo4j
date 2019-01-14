/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.planner.v3_5.spi

import org.neo4j.cypher.internal.v3_5.util.Cardinality
import org.neo4j.cypher.internal.v3_5.util.LabelId
import org.neo4j.cypher.internal.v3_5.util.RelTypeId
import org.neo4j.cypher.internal.v3_5.util.Selectivity

object GraphStatistics {
  val DEFAULT_RANGE_SELECTIVITY          = Selectivity(0.3)
  val DEFAULT_PREDICATE_SELECTIVITY      = Selectivity(0.75)
  val DEFAULT_PROPERTY_SELECTIVITY       = Selectivity(0.5)
  val DEFAULT_EQUALITY_SELECTIVITY       = Selectivity(0.1)
  val DEFAULT_TYPE_SELECTIVITY           = Selectivity(0.9)
  val DEFAULT_NUMBER_OF_ID_LOOKUPS       = Cardinality(25)
  val DEFAULT_LIST_CARDINALITY           = Cardinality(25)
  val DEFAULT_LIMIT_CARDINALITY          = Cardinality(75)
  val DEFAULT_REL_UNIQUENESS_SELECTIVITY = Selectivity(1.0 - 1 / 100 /*rel-cardinality*/)
  val DEFAULT_RANGE_SEEK_FACTOR          = 0.03
  val DEFAULT_STRING_LENGTH              = 6
  val DEFAULT_DISTINCT_SELECTIVITY       = Selectivity(0.95)
}

trait GraphStatistics {

  /**
    * Gets the Cardinality for given LabelId
    *
    * Attention: This method does NOT return the number of nodes anymore!
    * @param labelId Either some labelId for which the Cardinality should be retrieved or None
    * @return returns the Cardinality for the given LabelId or Cardinality(1) for a non-existing label
    */
  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality

  def nodesAllCardinality(): Cardinality

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality

  /*
      Probability of any node in the index to have a given property with a particular value

      indexSelectivity(:X, prop) = s => |MATCH (a:X)  WHERE has(x.prop)| * s = |MATCH (a:X) WHERE x.prop = '*'|
   */
  def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity]

  /*
      Probability of any node with the given label, to have a particular property

      indexPropertyExistsSelectivity(:X, prop) = s => |MATCH (a:X)| * s = |MATCH (a:X) WHERE has(x.prop)|
   */
  def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity]
}

class DelegatingGraphStatistics(delegate: GraphStatistics) extends GraphStatistics {
  override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    delegate.nodesWithLabelCardinality(labelId)

  override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    delegate.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel)

  override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
    delegate.uniqueValueSelectivity(index)

  override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
    delegate.indexPropertyExistsSelectivity(index)

  override def nodesAllCardinality(): Cardinality = delegate.nodesAllCardinality()
}

class StatisticsCompletingGraphStatistics(delegate: GraphStatistics)
  extends DelegatingGraphStatistics(delegate) {

  override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    (fromLabel, toLabel) match {
      case (Some(_), Some(_)) =>
        // TODO: read real counts from readOperations when they are gonna be properly computed and updated
        Cardinality.min(
          super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, None),
          super.cardinalityByLabelsAndRelationshipType(None, relTypeId, toLabel)
        )
      case _ =>
        super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel)
    }
}

