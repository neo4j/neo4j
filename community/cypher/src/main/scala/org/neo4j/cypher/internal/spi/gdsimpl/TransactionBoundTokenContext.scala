/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi.TokenContext
import org.neo4j.kernel.api.exceptions.{PropertyKeyNotFoundException, LabelNotFoundKernelException}
import org.neo4j.kernel.api.operations.KeyReadOperations
import org.neo4j.kernel.api.BaseStatement

abstract class TransactionBoundTokenContext(statement:BaseStatement) extends TokenContext
{
  def getOptPropertyKeyId(propertyKeyName: String): Option[Long] =
    TokenContext.tryGet[PropertyKeyNotFoundException](getPropertyKeyId(propertyKeyName))

  def getPropertyKeyId(propertyKeyName: String) = {
    val propertyId: Long = statement.propertyKeyGetForName(propertyKeyName)
    if(propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY)
    {
      throw new PropertyKeyNotFoundException("No such property.", null)
    }
    propertyId
  }

  def getPropertyKeyName(propertyKeyId: Long): String = statement.propertyKeyGetName(propertyKeyId)

  def getLabelId(labelName: String): Long = {
    val labelId: Long = statement.labelGetForName(labelName)
    if(labelId == KeyReadOperations.NO_SUCH_LABEL)
    {
      throw new LabelNotFoundKernelException("No such label", null)
    }
    labelId
  }

  def getOptLabelId(labelName: String): Option[Long] =
    TokenContext.tryGet[LabelNotFoundKernelException](getLabelId(labelName))

  def getLabelName(labelId: Long): String = statement.labelGetName(labelId)
}