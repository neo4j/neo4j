/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, NameId, PropertyKeyId, RelTypeId}
import org.neo4j.kernel.api.{Statement => KernelStatement}

class TransactionBoundGraphStatistics(statement: KernelStatement) extends GraphStatistics {
  import TransactionBoundGraphStatistics.WILDCARD
  import TransactionBoundGraphStatistics.toKernelEncode

  def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
    HardcodedGraphStatistics.indexSelectivity(label,property)

  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    statement.readOperations().countsForNode(labelId)

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    (fromLabel, toLabel) match {
      case (Some(_), Some(_)) =>
        // TODO: read real counts from readOperations when they are gonna be properly computed and updated
        Math.min(
          statement.readOperations().countsForRelationship(fromLabel, relTypeId, WILDCARD ),
          statement.readOperations().countsForRelationship(WILDCARD, relTypeId, toLabel)
        )
      case _ =>
        statement.readOperations().countsForRelationship(fromLabel, relTypeId, toLabel)
    }
}

object TransactionBoundGraphStatistics {
  val WILDCARD: Int = -1

  private implicit def toKernelEncode(nameId: NameId): Int =
    nameId.id

  private implicit def toKernelEncode(nameId: Option[NameId]): Int =
    nameId.map(toKernelEncode).getOrElse(WILDCARD)
}
