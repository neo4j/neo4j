/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{GraphStatistics, StatisticsCompletingGraphStatistics}
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, NameId, PropertyKeyId, RelTypeId}
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.kernel.api.{Statement => KernelStatement}

object TransactionBoundGraphStatistics {
  def apply(statement: KernelStatement) = new StatisticsCompletingGraphStatistics(new BaseTransactionBoundGraphStatistics(statement))

  private class BaseTransactionBoundGraphStatistics(statement: KernelStatement) extends GraphStatistics {

    import NameId._

    def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val indexDescriptor = new IndexDescriptor( label, property )
        val labeledNodes = statement.readOperations().countsForNode( label ).toDouble

        // Probability of any node with the given label, to have a property with a given value
        val indexEntrySelectivity = statement.readOperations( ).indexUniqueValuesSelectivity( indexDescriptor )
        val frequencyOfNodesWithSameValue = 1.0 / indexEntrySelectivity
        val indexSelectivity = frequencyOfNodesWithSameValue / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def indexPropertyExistsSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      try {
        val indexDescriptor = new IndexDescriptor( label, property )
        val labeledNodes = statement.readOperations().countsForNode( label ).toDouble

        // Probability of any node with the given label, to have a given property
        val indexSize = statement.readOperations( ).indexSize( indexDescriptor )
        val indexSelectivity = indexSize / labeledNodes

        Selectivity.of(indexSelectivity)
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      statement.readOperations().countsForNode(labelId)

    def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      statement.readOperations().countsForRelationship(fromLabel, relTypeId, toLabel)
  }
}


