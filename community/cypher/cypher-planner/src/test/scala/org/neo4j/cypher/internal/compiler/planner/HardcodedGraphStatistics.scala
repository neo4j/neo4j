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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.planner.spi.{GraphStatistics, IndexDescriptor}
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.RelTypeId
import org.neo4j.cypher.internal.v4_0.util.Selectivity

case object HardcodedGraphStatistics extends GraphStatistics {
  private val NODES_CARDINALITY = Cardinality(10000)
  private val NODES_WITH_LABEL_SELECTIVITY = Selectivity.of(0.2).get
  private val NODES_WITH_LABEL_CARDINALITY = NODES_CARDINALITY * NODES_WITH_LABEL_SELECTIVITY
  private val PATTERN_STEP_CARDINALITY = Cardinality(50000)
  private val INDEX_SELECTIVITY = Selectivity.of(.02).get
  private val INDEX_PROPERTY_EXISTS_SELECTIVITY = Selectivity.of(.5).get

  def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
    Some(INDEX_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
    Some(INDEX_PROPERTY_EXISTS_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    labelId.map(_ => NODES_WITH_LABEL_CARDINALITY).getOrElse(Cardinality.SINGLE)

  def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    PATTERN_STEP_CARDINALITY

  override def nodesAllCardinality(): Cardinality = NODES_CARDINALITY
}
