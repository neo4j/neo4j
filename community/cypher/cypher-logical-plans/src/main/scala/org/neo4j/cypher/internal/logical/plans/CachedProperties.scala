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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.logical.plans.CachedProperties.Entry
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

case class CachedProperties(entries: Map[LogicalVariable, CachedProperties.Entry]) extends AnyVal {

  def addAll(additionalProperties: IterableOnce[CachedProperty]): CachedProperties = {
    val updatedEntries = additionalProperties.iterator.foldLeft(entries) {
      case (acc, cachedProperty: CachedProperty) => acc.get(cachedProperty.entityVariable) match {
          case Some(entry) => acc + (cachedProperty.entityVariable -> entry.addProperty(cachedProperty.propertyKey))
          case None => acc + (cachedProperty.entityVariable -> CachedProperties.Entry(
              cachedProperty.originalEntity,
              cachedProperty.entityType,
              Set(cachedProperty.propertyKey)
            ))
        }
    }
    CachedProperties(updatedEntries)
  }

  def add(
    logicalVariable: LogicalVariable,
    entityType: EntityType,
    propertyKeys: Set[PropertyKeyName]
  ): CachedProperties = {
    val updatedEntries = entries.updatedWith(logicalVariable) {
      case Some(entry) => Some(entry.copy(properties = entry.properties.union(propertyKeys)))
      case None        => Some(Entry(logicalVariable, entityType, propertyKeys))
    }
    CachedProperties(updatedEntries)
  }

  def union(other: CachedProperties): CachedProperties = {
    CachedProperties(entries.fuse(other.entries)(_ union _))
  }

  /**
   * Caches only common properties for a given variable.
   * For example if this is ((x)->(x,CTNode,(foo,bar)),(y)->(y,CTNode,(baz))) and other is ((x)->(x,CTNode,(foo)),(z)->(y,CTNode,(baz)))
   * then the output would be ((x)->(x,CTNode,(foo)),(y)->(y,CTNode,(baz)),(z)->(y,CTNode,(baz)))
   *
   * @param other - the cached properties to intersect
   * @return the common properties.
   */
  def intersectProperties(other: CachedProperties): CachedProperties = {
    val updatedEntries =
      (this.entries.keySet ++ other.entries.keySet).foldLeft(Map.empty[LogicalVariable, CachedProperties.Entry]) {
        case (acc, cachedVariable: LogicalVariable) =>
          (this.entries.get(cachedVariable), other.entries.get(cachedVariable)) match {
            case (Some(thisCachedEntry), Some(otherCachedEntry)) => thisCachedEntry.intersect(otherCachedEntry)
                .map(commonEntries => acc + (cachedVariable -> commonEntries))
                .getOrElse(acc)
            case (Some(thisCachedEntry), None)  => acc + (cachedVariable -> thisCachedEntry)
            case (None, Some(otherCachedEntry)) => acc + (cachedVariable -> otherCachedEntry)
            case _                              => acc
          }
      }
    CachedProperties(updatedEntries)
  }

  /**
   * Caches only common entries.
   * For example if this is ((x)->(x,CTNode,(foo,bar)),(y)->(y,CTNode,(baz))) and other is ((x)->(x,CTNode,(foo)),(z)->(y,CTNode,(baz)))
   * then the output would be ((x)->(x,CTNode,(foo)))
   *
   * @param other - the cached properties to intersect
   * @return the common entries.
   */
  def intersect(other: CachedProperties): CachedProperties = {
    val commonProperties = this.entries.foldLeft(Map.empty[LogicalVariable, Entry]) {
      case (acc, (logicalVariable, entry)) if other.entries.contains(logicalVariable) =>
        entry.intersect(other.entries(logicalVariable)) match {
          case Some(commonEntry) => acc + (logicalVariable -> commonEntry)
          case _                 => acc
        }
      case (acc, _) => acc
    }
    CachedProperties(commonProperties)
  }

  def contains(entityVariable: LogicalVariable, propertyKey: PropertyKeyName): Boolean =
    entries.get(entityVariable).exists(_.properties.contains(propertyKey))

  def propertiesNotYetCached(
    entityVariable: LogicalVariable,
    propertyKeys: Set[PropertyKeyName]
  ): Set[PropertyKeyName] = entries.get(entityVariable) match {
    case Some(entry) => propertyKeys.diff(entry.properties)
    case None        => propertyKeys
  }

  def rename(renamedToCurrentVariablesMap: Map[LogicalVariable, LogicalVariable]): CachedProperties = {
    val renamedVariables = renamedToCurrentVariablesMap.values.toSet
    CachedProperties(
      this.entries.foldLeft(CachedProperties.empty.entries) {
        case (acc, (logicalVariable, entry)) =>
          renamedToCurrentVariablesMap.get(logicalVariable) match {
            case Some(renamedVariable)                               => acc + (renamedVariable -> entry)
            case None if !renamedVariables.contains(logicalVariable) => acc + (logicalVariable -> entry)
            case None                                                => acc
          }
      }
    )
  }

  /**
   * This method only retains the variables and properties present in the map. It will prune out everything else.
   * This is currently useful in very few and specific cases, where the runtime clears up memory to only retain grouping keys required in an eager aggregation.
   *
   * @param variableToPropertyNames - the variables and properties to retain.
   * @return pruned cached properties
   */
  def retain(variableToPropertyNames: Map[LogicalVariable, Set[PropertyKeyName]]): CachedProperties = {
    CachedProperties(
      this.entries.collect {
        case (variable, entry) if variableToPropertyNames.contains(variable) =>
          variable -> entry.copy(properties = variableToPropertyNames(variable))
      }
    )
  }

  def nonEmpty: Boolean = this.entries.nonEmpty
  def isEmpty: Boolean = this.entries.isEmpty
}

object CachedProperties {

  case class Entry(originalEntity: LogicalVariable, entityType: EntityType, properties: Set[PropertyKeyName]) {

    def addProperty(property: PropertyKeyName): Entry =
      copy(properties = properties.incl(property))

    // We check for consistency of original entity and entity type, however this seems to be academic mainly, since all the variables are renamed during planning to avoid clashes.
    def intersect(otherEntry: Entry): Option[Entry] = {
      val commonProperties = properties.intersect(otherEntry.properties)
      if (
        otherEntry.originalEntity == this.originalEntity && otherEntry.entityType == this.entityType && commonProperties.nonEmpty
      )
        Some(copy(properties = commonProperties))
      else
        None
    }

    // TODO: do we need to check the consistency of originalEntity and entityType?
    def union(otherEntry: Entry): Entry =
      copy(properties = properties.union(otherEntry.properties))

  }

  def empty: CachedProperties = CachedProperties(Map.empty)

  def singleton(
    entityVariable: LogicalVariable,
    originalEntity: LogicalVariable,
    entityType: EntityType,
    properties: Set[PropertyKeyName]
  ): CachedProperties =
    CachedProperties(Map(entityVariable -> Entry(originalEntity, entityType, properties)))
}
