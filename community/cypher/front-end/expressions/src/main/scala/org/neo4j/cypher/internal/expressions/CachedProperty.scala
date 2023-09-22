/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.ASTCachedProperty.RuntimeKey
import org.neo4j.cypher.internal.util.InputPosition

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

  override def map: Expression = Variable(entityName)(this.position)

  /**
   * @return a textual representation of the entity and the property in the form `n.prop`
   */
  def propertyAccessString: String = s"$entityName.${propertyKey.name}"

  /**
   * @return the runtime key to be used when using this cached property in maps or sets at runtime,
   *         if different cached properties (different implementation or different current variable name)
   *         should be treated as if they were equal.
   */
  def runtimeKey: RuntimeKey = RuntimeKey(originalEntityName, propertyKey, entityType)(entityName)
}

object ASTCachedProperty {

  /**
   * Used to match equivalent cached properties at runtime.
   */
  case class RuntimeKey(originalEntityName: String, propertyKey: PropertyKeyName, entityType: EntityType)(
    val entityName: String
  ) {

    /**
     * @return a textual representation of the entity and the property in the form `n.prop`
     */
    def propertyAccessString: String = s"$entityName.${propertyKey.name}"

    def asCanonicalStringVal: String = s"cache[$propertyAccessString]"
  }
}

/**
 * A property value that is cached in the execution context. Such a value can be
 * retrieved very fast, but care has to be taken to it doesn't get out-dated by writes to
 * the graph/transaction state.
 *
 * @param originalEntity     the name of the entity how it first appeared in the query, that is, before any projections.
 * @param entityVariable     the variable how it appeared in this particular Property. It can have a different name than `originalEntityName`,
 *                           if the variable name was changed in between.
 * @param knownToAccessStore `true` if we know that the evaluation of this CachedProperty will access the store, `false` otherwise.
 *                           This is purely used for Cost estimation and has no effect on the runtime.
 */
case class CachedProperty(
  originalEntity: LogicalVariable,
  entityVariable: LogicalVariable,
  override val propertyKey: PropertyKeyName,
  override val entityType: EntityType,
  knownToAccessStore: Boolean = false
)(val position: InputPosition) extends ASTCachedProperty {

  override val entityName: String = entityVariable.name
  override def originalEntityName: String = originalEntity.name

  override def asCanonicalStringVal: String = s"cache[$propertyAccessString]"
}

/**
 * A specialized version of `CachedProperty` that doesn't read the actual value but only produces
 * `TRUE` if the property is there or `NULL` otherwise.
 *
 * @param originalEntity the name of the entity how it first appeared in the query, that is, before any projections.
 * @param entityVariable the variable how it appeared in this particular Property. It can have a different name than `originalEntityName`,
 *                       if the variable name was changed in between.
 */
case class CachedHasProperty(
  originalEntity: LogicalVariable,
  entityVariable: LogicalVariable,
  override val propertyKey: PropertyKeyName,
  override val entityType: EntityType,
  knownToAccessStore: Boolean = false
)(val position: InputPosition) extends ASTCachedProperty {

  override val entityName: String = entityVariable.name
  override def originalEntityName: String = originalEntity.name

  override def asCanonicalStringVal: String = s"cache[$propertyAccessString]"
}
