/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.opencypher.v9_0.ast.semantics.{SemanticCheck, SemanticCheckResult, SemanticCheckableExpression}
import org.opencypher.v9_0.expressions.{LogicalVariable, PropertyKeyName, Expression => ASTExpression}
import org.opencypher.v9_0.util.{InputPosition, InternalException}

/**
  * A node property value that is cached in the execution context. Such a value can be
  * retrieved very fast, but care has to be taken to it doesn't out-dated by writes to
  * the graph/transaction state.
  *
  * @param nodeVariableName the node variable
  * @param propertyKey the property key
  * @param position
  */
case class CachedNodeProperty(nodeVariableName: String,
                              propertyKey: PropertyKeyName
                            )(val position: InputPosition) extends LogicalVariable with SemanticCheckableExpression {

  def asString = s"$nodeVariableName.${propertyKey.name}"

  override def name: String = s"$nodeVariableName.${propertyKey.name}"

  override def asCanonicalStringVal: String = s"`$name`"

  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success

  override def copyId = fail()

  override def renameId(newName: String) = fail()

  override def bumpId = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a CachedNodeProperty as Variable")
}
