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

import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.kernel.impl.util.dbstructure.DbStructureLookup

class DbStructureGraphStatistics(lookup: DbStructureLookup) extends GraphStatistics {

  override def nodesWithLabelCardinality( label: Option[LabelId] ): Cardinality =
    Cardinality(lookup.nodesWithLabelCardinality(label))

  override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId] ): Cardinality =
    Cardinality(lookup.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel))

  override def uniqueValueSelectivity(index: IndexDescriptor ): Option[Selectivity] = {
    val result = lookup.indexUniqueValueSelectivity( index.label.id, index.property.id )
    Selectivity.of(result)
  }

  override def indexPropertyExistsSelectivity( index: IndexDescriptor ): Option[Selectivity] = {
    val result = lookup.indexPropertyExistsSelectivity( index.label.id, index.property.id )
    if (result.isNaN) None else Some(Selectivity.of(result).get)
  }

  override def nodesAllCardinality(): Cardinality = Cardinality(lookup.nodesAllCardinality())
}
