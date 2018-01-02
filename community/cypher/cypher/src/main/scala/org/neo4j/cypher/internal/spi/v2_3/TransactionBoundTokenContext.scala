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

import org.neo4j.cypher.internal.compiler.v2_3.spi.TokenContext
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.exceptions.{LabelNotFoundKernelException, PropertyKeyNotFoundException, RelationshipTypeNotFoundException}
import org.neo4j.kernel.impl.api.operations.KeyReadOperations

abstract class TransactionBoundTokenContext(protected var statement: Statement) extends TokenContext {
  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = {
    val propertyId: Int = statement.readOperations().propertyKeyGetForName(propertyKeyName)
    if (propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY) None
    else Some(propertyId)
  }

  def getPropertyKeyId(propertyKeyName: String) = {
    val propertyId: Int = statement.readOperations().propertyKeyGetForName(propertyKeyName)
    if (propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY)
      throw new PropertyKeyNotFoundException("No such property.", null)
    propertyId
  }

  def getPropertyKeyName(propertyKeyId: Int): String = statement.readOperations().propertyKeyGetName(propertyKeyId)

  def getLabelId(labelName: String): Int = {
    val labelId: Int = statement.readOperations().labelGetForName(labelName)
    if (labelId == KeyReadOperations.NO_SUCH_LABEL)
      throw new LabelNotFoundKernelException("No such label", null)
    labelId
  }

  def getOptLabelId(labelName: String): Option[Int] = {
    val labelId: Int = statement.readOperations().labelGetForName(labelName)
    if (labelId == KeyReadOperations.NO_SUCH_LABEL) None
    else Some(labelId)
  }

  def getLabelName(labelId: Int): String = statement.readOperations().labelGetName(labelId)

  def getOptRelTypeId(relType: String): Option[Int] = {
    val relTypeId: Int = statement.readOperations().relationshipTypeGetForName(relType)
    if (relTypeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE) None
    else Some(relTypeId)
  }

  def getRelTypeId(relType: String): Int = {
    val relTypeId: Int = statement.readOperations().relationshipTypeGetForName(relType)
    if (relTypeId == KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE)
      throw new RelationshipTypeNotFoundException("No such relationship.", null)
    relTypeId
  }

  def getRelTypeName(id: Int): String = statement.readOperations().relationshipTypeGetName(id)
}
