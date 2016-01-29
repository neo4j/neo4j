/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner.CreatesPropertyKeys
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast.{LabelName, RelTypeName}

trait Update {

  def overlaps(read: Read): Boolean = nonEmpty && (nodeOverlaps(read) || relOverlaps(read))

  private def nodeOverlaps(read: Read): Boolean = {
    val ids = read.nodeIds
    read.readsNodes && (deletesOtherThan(read.graphEntities) || ids.exists { nodeId =>
      val readLabels = read.labelsOn(nodeId)
      val readProps = read.propertiesOn(nodeId)

      val a = if (read.readsRelationships)
        false
      else {
        val readsNoLabels = readLabels.isEmpty
        val readsNoProps = readProps.isEmpty
        readsNoLabels && readsNoProps && createsNodes
      }

      val b = {
        val updatedLabels = addedLabelsNotOn(nodeId) ++ removedLabelsNotOn(nodeId)
        readLabels containsAnyOf updatedLabels
      }

      val c = {
        val updatedProperties = updatesNodePropertiesNotOn(nodeId)
        readProps exists updatedProperties.overlaps
      }

      a || b || c || deletes(nodeId)
    })
  }

  private def relOverlaps(read: Read): Boolean = {
    updatesRelationships && read.readsRelationships && read.relationships.exists { rel =>
      val readTypes = rel.types.toSet
      val readProps = read.propertiesOn(rel.name)
      val updatedProperties = if (rel.dir == SemanticDirection.BOTH)
        allRelationshipPropertyUpdates
      else
        relationshipPropertyUpdatesNotOn(rel.name)
      val createdTypes = relTypesCreated

      val a = {
        val readsNoTypes = readTypes.isEmpty
        val readsNoProps = readProps.isEmpty
        readsNoTypes && readsNoProps && createsRelationships
      }

      val b = {
        readTypes.nonEmpty && (readTypes containsAnyOf createdTypes)
      }

      val c = {
        readProps.nonEmpty && (readProps exists updatedProperties.overlaps)
      }

      val d = {
        deletes(rel.name) && rel.dir == SemanticDirection.BOTH
      }

      val e = readTypes.isEmpty && c
      val g = readProps.isEmpty && b
      a || e || g || d || (b && c)
    }
  }

  def createsNodes: Boolean
  def createsRelationships: Boolean
  def updatesRelationships: Boolean
  def deletes(name: IdName): Boolean
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty

  def addedLabelsNotOn(id: IdName): Set[LabelName]
  def removedLabelsNotOn(id: IdName): Set[LabelName]

  def updatesNodePropertiesNotOn(id: IdName): CreatesPropertyKeys
  def relationshipPropertyUpdatesNotOn(id: IdName): CreatesPropertyKeys
  def allRelationshipPropertyUpdates: CreatesPropertyKeys
  def containsDeletes: Boolean
  def deletesOtherThan(ids: Set[IdName]): Boolean

  def relTypesCreated: Set[RelTypeName]

  implicit class apa[T](my: Set[T]) {
    def containsAnyOf(other:Set[T]) = (my intersect other).nonEmpty
  }
}
