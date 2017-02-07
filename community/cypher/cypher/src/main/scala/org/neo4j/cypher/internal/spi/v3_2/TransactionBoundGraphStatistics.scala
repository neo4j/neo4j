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
package org.neo4j.cypher.internal.spi.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, StatisticsCompletingGraphStatistics}
import org.neo4j.cypher.internal.frontend.v3_2.{LabelId, NameId, PropertyKeyId, RelTypeId}
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, Selectivity}
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException
import org.neo4j.kernel.api.schema.IndexDescriptorFactory

object TransactionBoundGraphStatistics {
  def apply(ops: ReadOperations) = new StatisticsCompletingGraphStatistics(new BaseTransactionBoundGraphStatistics(ops))

  private class BaseTransactionBoundGraphStatistics(operations: ReadOperations) extends GraphStatistics {

    import NameId._

    def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val indexDescriptor = IndexDescriptorFactory.of( label, property )
        val labeledNodes = operations.countsForNodeWithoutTxState( label ).toDouble

        // Probability of any node with the given label, to have a property with a given value
        val indexEntrySelectivity = operations.indexUniqueValuesSelectivity(indexDescriptor)
        val frequencyOfNodesWithSameValue = 1.0 / indexEntrySelectivity
        val indexSelectivity = frequencyOfNodesWithSameValue / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def indexPropertyExistsSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val indexDescriptor = IndexDescriptorFactory.of( label, property )
        val labeledNodes = operations.countsForNodeWithoutTxState( label ).toDouble

        // Probability of any node with the given label, to have a given property
        val indexSize = operations.indexSize(indexDescriptor)
        val indexSelectivity = indexSize / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      atLeastOne(operations.countsForNodeWithoutTxState(labelId))

    def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      atLeastOne(operations.countsForRelationshipWithoutTxState(fromLabel, relTypeId, toLabel))

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


