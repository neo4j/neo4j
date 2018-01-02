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
package org.neo4j.cypher.internal.spi.v3_3

import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.api.exceptions.{PropertyKeyNotFoundException, RelationshipTypeNotFoundException}
import org.neo4j.kernel.impl.api.operations.KeyReadOperations

abstract class TransactionBoundTokenContext(readOperationsSupplier: () => ReadOperations) extends TokenContext {
  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
    val propertyId: Int = readOperationsSupplier().propertyKeyGetForName(propertyKeyName)
    if (propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY) None
    else Some(propertyId)
  }

  def getPropertyKeyId(propertyKeyName: String): Int = {
    val propertyId: Int = readOperationsSupplier().propertyKeyGetForName(propertyKeyName)
    if (propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY)
      throw new PropertyKeyNotFoundException("No such property.", null)
    propertyId
  }

  def getPropertyKeyName(propertyKeyId: Int): String = readOperationsSupplier().propertyKeyGetName(propertyKeyId)

  def getLabelId(labelName: String): Int = {
    val labelId: Int = readOperationsSupplier().labelGetForName(labelName)
    if (labelId == KeyReadOperations.NO_SUCH_LABEL)
      throw new LabelNotFoundKernelException("No such label", null)
    labelId
  }

  def getOptLabelId(labelName: String): Option[Int] = {
    val labelId: Int = readOperationsSupplier().labelGetForName(labelName)
    if (labelId == KeyReadOperations.NO_SUCH_LABEL) None
    else Some(labelId)
  }

  def getLabelName(labelId: Int): String = readOperationsSupplier().labelGetName(labelId)

  def getOptRelTypeId(relType: String): Option[Int] = {
    val relTypeId: Int = readOperationsSupplier().relationshipTypeGetForName(relType)
    if (relTypeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE) None
    else Some(relTypeId)
  }

  def getRelTypeId(relType: String): Int = {
    val relTypeId: Int = readOperationsSupplier().relationshipTypeGetForName(relType)
    if (relTypeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE)
      throw new RelationshipTypeNotFoundException("No such relationship.", null)
    relTypeId
  }

  def getRelTypeName(id: Int): String = readOperationsSupplier().relationshipTypeGetName(id)
}
