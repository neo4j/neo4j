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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException
import org.neo4j.kernel.api.exceptions.RelationshipTypeNotFoundException

abstract class TransactionBoundReadTokenContext(transactionalContext: QueryTransactionalContext)
    extends ReadTokenContext {

  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
    val propertyId: Int = transactionalContext.tokenRead.propertyKey(propertyKeyName)
    if (propertyId == TokenRead.NO_TOKEN) None
    else Some(propertyId)
  }

  def getPropertyKeyId(propertyKeyName: String): Int = {
    val propertyId: Int = transactionalContext.tokenRead.propertyKey(propertyKeyName)
    if (propertyId == TokenRead.NO_TOKEN)
      throw new PropertyKeyNotFoundException(propertyKeyName, null)
    propertyId
  }

  def getPropertyKeyName(propertyKeyId: Int): String = transactionalContext.tokenRead.propertyKeyName(propertyKeyId)

  def getLabelId(labelName: String): Int = {
    val labelId: Int = transactionalContext.tokenRead.nodeLabel(labelName)
    if (labelId == TokenRead.NO_TOKEN)
      throw new LabelNotFoundKernelException(labelId, null)
    labelId
  }

  def getOptLabelId(labelName: String): Option[Int] = {
    val labelId: Int = transactionalContext.tokenRead.nodeLabel(labelName)
    if (labelId == TokenRead.NO_TOKEN) None
    else Some(labelId)
  }

  def getLabelName(labelId: Int): String = transactionalContext.tokenRead.nodeLabelName(labelId)

  def getOptRelTypeId(relType: String): Option[Int] = {
    val relTypeId: Int = transactionalContext.tokenRead.relationshipType(relType)
    if (relTypeId == TokenRead.NO_TOKEN) None
    else Some(relTypeId)
  }

  def getRelTypeId(relType: String): Int = {
    val relTypeId: Int = transactionalContext.tokenRead.relationshipType(relType)
    if (relTypeId == TokenRead.NO_TOKEN)
      throw new RelationshipTypeNotFoundException(relType, null)
    relTypeId
  }

  def getRelTypeName(id: Int): String = transactionalContext.tokenRead.relationshipTypeName(id)
}
