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
package org.neo4j.cypher.internal.spi.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v3_1.spi.{GraphStatistics, StatisticsCompletingGraphStatistics}
import org.neo4j.cypher.internal.frontend.v3_1.{LabelId, NameId, PropertyKeyId, RelTypeId}
import org.neo4j.internal.kernel.api.{Read, SchemaRead}
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException
import org.neo4j.kernel.impl.api.store.DefaultIndexReference

object TransactionBoundGraphStatistics {
  def apply(read: Read, schemaRead: SchemaRead) = new StatisticsCompletingGraphStatistics(new BaseTransactionBoundGraphStatistics(read, schemaRead))

  private class BaseTransactionBoundGraphStatistics(read: Read, schemaRead: SchemaRead) extends GraphStatistics {

    import NameId._

    def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val labeledNodes = read.countsForNodeWithoutTxState( label ).toDouble

        // Probability of any node with the given label, to have a property with a given value
        val indexEntrySelectivity = schemaRead.indexUniqueValuesSelectivity(
          DefaultIndexReference.general(label.id, property.id)
        )
        val frequencyOfNodesWithSameValue = 1.0 / indexEntrySelectivity
        val indexSelectivity = frequencyOfNodesWithSameValue / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def indexPropertyExistsSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val labeledNodes = read.countsForNodeWithoutTxState( label ).toDouble

        // Probability of any node with the given label, to have a given property
        val indexSize = schemaRead.indexSize(DefaultIndexReference.general(label.id, property.id))
        val indexSelectivity = indexSize / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      atLeastOne(read.countsForNodeWithoutTxState(labelId))

    def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      atLeastOne(read.countsForRelationshipWithoutTxState(fromLabel, relTypeId, toLabel))

    /**
      * Due to the way cardinality calculations work, zero is a bit dangerous, as it cancels out
      * any cost that it multiplies with. To avoid this pitfall, we determine that the least count
      * available is one, not zero.
      */
    private def atLeastOne(count: Double): Cardinality = {
      if (count < 1)
        Cardinality.SINGLE
      else
        Cardinality(count)
    }
  }
}


