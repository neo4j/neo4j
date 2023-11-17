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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

trait GraphStatistics {

  /**
   * Gets the Cardinality of all nodes regardless of labels.
   */
  def nodesAllCardinality(): Cardinality

  def mostCommonLabelGivenRelationshipType(typ: Int): Seq[Int] = Seq.empty

  /**
   * Gets the Cardinality for given LabelId
   *
   * Attention: This method does NOT return the number of nodes anymore!
   * @param labelId Either some labelId for which the Cardinality should be retrieved or None
   * @return returns the Cardinality for the given LabelId or Cardinality(1) for a non-existing label
   */
  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality

  /**
   * Gets the Cardinality of all relationships (a)-[r]->(b), where
   *
   * {{{
   *   a has the label `fromLabel`, or any labels if `fromLabel` is None
   *   b has the label `toLabel`, or any labels if `toLabel` is None
   *   r has the type `relTypeId`, or any type if `relTypeId` is None
   * }}}
   */
  def patternStepCardinality(
    fromLabel: Option[LabelId],
    relTypeId: Option[RelTypeId],
    toLabel: Option[LabelId]
  ): Cardinality

  /**
   * Probability of any node in the index to have a given property with a particular value.
   *
   * uniqueValueSelectivity(:X, prop) = s => |MATCH (a:X)  WHERE x.prop IS NOT NULL| * s = |MATCH (a:X) WHERE x.prop = '*'|
   */
  def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity]

  /**
   * Probability of any node or relationship with the given label or relType, to have a particular property.
   *
   * indexPropertyExistsSelectivity(:X, prop) = s => |MATCH (a:X)| * s = |MATCH (a:X) WHERE x.prop IS NOT NULL|
   */
  def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity]
}

class DelegatingGraphStatistics(delegate: GraphStatistics) extends GraphStatistics {

  override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    delegate.nodesWithLabelCardinality(labelId)

  override def patternStepCardinality(
    fromLabel: Option[LabelId],
    relTypeId: Option[RelTypeId],
    toLabel: Option[LabelId]
  ): Cardinality =
    delegate.patternStepCardinality(fromLabel, relTypeId, toLabel)

  override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
    delegate.uniqueValueSelectivity(index)

  override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] =
    delegate.indexPropertyIsNotNullSelectivity(index)

  override def nodesAllCardinality(): Cardinality = delegate.nodesAllCardinality()

  override def mostCommonLabelGivenRelationshipType(typ: Int): Seq[Int] =
    delegate.mostCommonLabelGivenRelationshipType(typ)
}
