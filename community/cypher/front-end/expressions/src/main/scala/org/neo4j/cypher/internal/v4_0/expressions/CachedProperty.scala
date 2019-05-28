/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.expressions

import org.neo4j.cypher.internal.v4_0.util.InputPosition

import scala.util.hashing.MurmurHash3

sealed trait CachedType

case object CACHED_NODE extends CachedType

case object CACHED_RELATIONSHIP extends CachedType

/**
  * Common super class of [[CachedProperty]]
  * and its slotted specializations.
  */
trait ASTCachedProperty extends LogicalProperty {
  def cachedType: CachedType

  def variableName: String

  def propertyKey: PropertyKeyName

  override val map: Expression = Variable(variableName)(this.position)

  def cacheKey: String = s"$variableName.${propertyKey.name}"
}

/**
  * A property value that is cached in the execution context. Such a value can be
  * retrieved very fast, but care has to be taken to it doesn't out-dated by writes to
  * the graph/transaction state.
  *
  * @param variableName the variable
  * @param usedVariable the variable how it appeared in the original Property. It can have a different name than `variableName`.
  * @param propertyKey  the property key
  */
case class CachedProperty(variableName: String,
                          usedVariable: LogicalVariable,
                          propertyKey: PropertyKeyName,
                          override val cachedType: CachedType
                         )(val position: InputPosition) extends ASTCachedProperty {

  override def asCanonicalStringVal: String = s"cache[$cacheKey]"

  override def hashCode(): Int = MurmurHash3.seqHash(Seq(variableName, propertyKey, cachedType))

  override def equals(obj: Any): Boolean = obj match {
    case other:CachedProperty => Seq(variableName, propertyKey, cachedType) == Seq(other.variableName, other.propertyKey, other.cachedType)
    case _ => false
  }
}
