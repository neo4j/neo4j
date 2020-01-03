/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

sealed trait EntityType

case object NODE_TYPE extends EntityType

case object RELATIONSHIP_TYPE extends EntityType

/**
  * Common super class of [[CachedProperty]]
  * and its slotted specializations.
  */
trait ASTCachedProperty extends LogicalProperty {
  /**
    * @return the type of the entity for which a property is cached.
    */
  def entityType: EntityType

  /**
    * @return the original name of the entity for which a property is cached.
    */
  def originalEntityName: String

  /**
    * @return the name of the entity for which a property is cached.
    */
  def entityName: String

  /**
    * @return the property key name
    */
  def propertyKey: PropertyKeyName

  override val map: Expression = Variable(entityName)(this.position)

  /**
    * @return a textual representation of the entity and the property in the form `n.prop`
    */
  def propertyAccessString: String = s"$entityName.${propertyKey.name}"

  // CachedProperties are stored as keys in `MapExecutionContext`. The lookup is by original entity name and property key.
  // Therefore, we need to override equality and hashCode to disregard `entityVariable`
  override final def equals(obj: Any): Boolean = obj match {
    case other:ASTCachedProperty => Seq(originalEntityName, propertyKey, entityType) == Seq(other.originalEntityName, other.propertyKey, other.entityType)
    case _ => false
  }

  override final def hashCode(): Int = MurmurHash3.seqHash(Seq(originalEntityName, propertyKey, entityType))
}

/**
  * A property value that is cached in the execution context. Such a value can be
  * retrieved very fast, but care has to be taken to it doesn't out-dated by writes to
  * the graph/transaction state.
  *
  * @param originalEntityName the name of the variable how it appeared in the first Property access.
  * @param entityVariable     the variable how it appeared in this particular Property. It can have a different name than `originalEntityName`,
  *                           if the variable name was changed in between.
  */
case class CachedProperty(override val originalEntityName: String,
                          entityVariable: LogicalVariable,
                          override val propertyKey: PropertyKeyName,
                          override val entityType: EntityType)(val position: InputPosition) extends ASTCachedProperty {

  override val entityName: String = entityVariable.name

  override def asCanonicalStringVal: String = s"cache[$propertyAccessString]"
}
