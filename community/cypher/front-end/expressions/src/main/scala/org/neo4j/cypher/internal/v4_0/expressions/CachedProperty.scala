/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
