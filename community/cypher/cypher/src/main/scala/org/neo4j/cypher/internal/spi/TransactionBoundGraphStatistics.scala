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
package org.neo4j.cypher.internal.spi

import java.lang.Math.min

import org.neo4j.cypher.internal.planner.spi.{GraphStatistics, IndexDescriptor, MinimumGraphStatistics}
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, LabelId, RelTypeId, Selectivity}
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.{Read, SchemaRead, TokenRead}
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.kernel.impl.query.TransactionalContext

object TransactionBoundGraphStatistics {
  def apply(transactionalContext: TransactionalContext): MinimumGraphStatistics =
    apply(transactionalContext.kernelTransaction().dataRead(), transactionalContext.kernelTransaction().schemaRead())

  def apply(read: Read, schemaRead: SchemaRead): MinimumGraphStatistics = {
    new MinimumGraphStatistics(new BaseTransactionBoundGraphStatistics(read, schemaRead))
  }

  private class BaseTransactionBoundGraphStatistics(read: Read, schemaRead: SchemaRead) extends GraphStatistics with IndexDescriptorCompatibility {

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
      try {
        val indexDescriptor = Iterators.single(schemaRead.index(SchemaDescriptor.forLabel(index.label, index.properties.map(_.id): _*)),
          org.neo4j.internal.schema.IndexDescriptor.NO_INDEX)
        val indexSize = schemaRead.indexSize(indexDescriptor)
        if (indexSize == 0)
          Some(Selectivity.ZERO)
        else {
          // Probability of any node in the index, to have a property with a given value
          val indexEntrySelectivity = schemaRead.indexUniqueValuesSelectivity(indexDescriptor)
          if (indexEntrySelectivity == 0.0) {
            Some(Selectivity.ZERO)
          } else {
            val frequencyOfNodesWithSameValue = 1.0 / indexEntrySelectivity

            // This is = 1 / number of unique values
            val indexSelectivity = frequencyOfNodesWithSameValue / indexSize

            Selectivity.of(min(indexSelectivity, 1.0))
          }
        }
      }
      catch {
        case _: IndexNotFoundKernelException => None
      }

    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
      try {
        val labeledNodes = read.countsForNodeWithoutTxState( index.label ).toDouble
        if (labeledNodes == 0)
          Some(Selectivity.ZERO)
        else {
          // Probability of any node with the given label, to have a given property
          val indexDescriptor = Iterators.single(schemaRead.index(SchemaDescriptor.forLabel(index.label, index.properties.map(_.id): _*)),
            org.neo4j.internal.schema.IndexDescriptor.NO_INDEX)
          val indexSize = schemaRead.indexSize(indexDescriptor)
          val indexSelectivity = indexSize / labeledNodes

          //Even though semantically impossible the index can get into a state where
          //the indexSize > labeledNodes
          Selectivity.of(min(indexSelectivity, 1.0))
        }
      }
      catch {
        case _: IndexNotFoundKernelException => None
      }

    override def nodesAllCardinality(): Cardinality =
      Cardinality(read.countsForNodeWithoutTxState(TokenRead.ANY_LABEL))

    override def nodesWithLabelCardinality(maybeLabelId: Option[LabelId]): Cardinality = {
      val count: Long = maybeLabelId.map(labelId => read.countsForNodeWithoutTxState(labelId.id)).getOrElse(0L)
      Cardinality(count)
    }

    override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
     Cardinality(read.countsForRelationshipWithoutTxState(fromLabel, relTypeId, toLabel))
  }
}
