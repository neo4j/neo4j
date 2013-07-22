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
import org.neo4j.kernel.api.StatementOperations
import org.neo4j.kernel.api.operations.StatementState
import org.neo4j.kernel.api.StatementOperationParts
import org.neo4j.kernel.api.operations.KeyReadOperations

abstract class TransactionBoundTokenContext(ctx: KeyReadOperations, state: StatementState) extends TokenContext
{
  def getOptPropertyKeyId(propertyKeyName: String): Option[Long] =
    TokenContext.tryGet[PropertyKeyNotFoundException](getPropertyKeyId(propertyKeyName))

  def getPropertyKeyId(propertyKeyName: String) = ctx.propertyKeyGetForName(state, propertyKeyName)

  def getPropertyKeyName(propertyKeyId: Long): String = ctx.propertyKeyGetName(state, propertyKeyId)

  def getLabelId(labelName: String): Long = ctx.labelGetForName(state, labelName)

  def getOptLabelId(labelName: String): Option[Long] =
    TokenContext.tryGet[LabelNotFoundKernelException](getLabelId(labelName))

  def getLabelName(labelId: Long): String = ctx.labelGetName(state, labelId)
}