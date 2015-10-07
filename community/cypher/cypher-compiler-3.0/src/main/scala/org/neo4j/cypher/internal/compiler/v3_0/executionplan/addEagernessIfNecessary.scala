/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan

import org.neo4j.cypher.internal.compiler.v3_0.pipes.{EagerPipe, Pipe}

object addEagernessIfNecessary extends (Pipe => Pipe) {
  private val DEBUG = false

  def apply(toPipe: Pipe): Pipe = {
    val sources = toPipe.sources.map(apply).map { fromPipe =>
      val from = fromPipe.effects
      val to = toPipe.localEffects
      if (wouldConflict(from, to)) {
        new EagerPipe(fromPipe)()(fromPipe.monitor)
      } else {
        fromPipe
      }
    }
    toPipe.dup(sources.toList)
  }

  private def wouldConflict(from: Effects, to: Effects): Boolean = {
    if (DEBUG) wouldConflictDebug(from, to)
    else {
      // NOTE: Leaf effects will not be considered unless effects have first been flattened with Effects.regardlessOfLeafEffects
      val fromWithoutLeafInfo = from.regardlessOfLeafEffects
      val toWithoutLeafInfo = to.regardlessOfLeafEffects

      nodesReadWriteConflict(from, to) || // NOTE: Here we should _not_ consider leaf effects
        nodesCreateReadConflict(from, toWithoutLeafInfo) || // creating in a leaf is fine; always one row
        nodesDeleteMergeConflict(fromWithoutLeafInfo, toWithoutLeafInfo) ||
        nodePropertiesConflict(from, to) ||
        relsReadWriteConflict(from, to) ||
        relsCreateReadConflict(from, toWithoutLeafInfo) ||
        relsDeleteMergeConflict(fromWithoutLeafInfo, toWithoutLeafInfo) ||
        relationshipPropertiesConflict(from, to)
    }
  }

  private def wouldConflictDebug(from: Effects, to: Effects): Boolean = {
    assert (DEBUG)

    // NOTE: Leaf effects will not be considered unless effects have first been flattened with Effects.regardlessOfLeafEffects
    val fromWithoutLeafInfo = from.regardlessOfLeafEffects
    val toWithoutLeafInfo = to.regardlessOfLeafEffects

    val nodeNonLeafReadWriteConflict = nodesReadWriteConflict(from, to) // NOTE: Here we should _not_ consider leaf effects
    val nodeCreateReadConflict = nodesCreateReadConflict(from, toWithoutLeafInfo) // creating in a leaf is fine; always one row
    val nodeDeleteMergeConflict = nodesDeleteMergeConflict(fromWithoutLeafInfo, toWithoutLeafInfo)
    val nodePropConflict = nodePropertiesConflict(from, to)

    val relNonLeafReadWriteConflict = relsReadWriteConflict(from, to)
    val relCreateReadConflict = relsCreateReadConflict(from, toWithoutLeafInfo)
    val relDeleteMergeConflict = relsDeleteMergeConflict(fromWithoutLeafInfo, toWithoutLeafInfo)
    val relPropConflict = relationshipPropertiesConflict(from, to)

    nodeNonLeafReadWriteConflict ||
      nodeCreateReadConflict ||
      nodeDeleteMergeConflict ||
      nodePropConflict ||
      relNonLeafReadWriteConflict ||
      relCreateReadConflict ||
      relDeleteMergeConflict ||
      relPropConflict
  }


  private def relsReadWriteConflict(from: Effects, to: Effects) = {
    readsCreatesSameRelationship(from.regardlessOfOptionalEffects, to) || readsDeletesSameRelationship(from, to)
  }

  private def relsCreateReadConflict(from: Effects, to: Effects) = {
    readsCreatesSameRelationship(to, from)
  }

  private def nodesCreateReadConflict(from: Effects, to: Effects) = {
    // Flip the order to reuse this code:
    readsCreatesSameNode(to, from)
  }

  def nodesReadWriteConflict(from: Effects, to: Effects) = {
    readsCreatesSameNode(from.regardlessOfOptionalEffects, to) || readsDeletesSameNode(from, to)
  }

  private def relsDeleteMergeConflict(from: Effects, to: Effects) = {
    from.contains(DeletesRelationship) && readsCreatesSameRelationship(from, to)
  }
  private def readsCreatesSameRelationship(from: Effects, to: Effects) = {
    from.contains(ReadsAllRelationships) && to.effectsSet.exists {
      case create: CreatesRelationships => true
      case _ => false
    } || readsCreatesSameRelationshipWithType(from, to)
  }

  private def readsCreatesSameRelationshipWithType(from: Effects, to: Effects) = {
    val fromTypes = from.effectsSet.collect {
      case ReadsRelationshipsWithTypes(types) => types
    }.flatten

    val toTypes = to.effectsSet.collect {
      case CreatesRelationship(typ) => typ
    }

    (fromTypes intersect toTypes).nonEmpty
  }

  private def nodesDeleteMergeConflict(from: Effects, to: Effects) = {
    from.contains(DeletesNode) && readsCreatesSameNode(from, to)
  }

  private def readsCreatesSameNode(from: Effects, to: Effects) = {
    from.contains(ReadsAllNodes) && to.effectsSet.exists {
      case create: CreatesNodes => true
      case _ => false
    } || readsCreatesSameNodeWithLabels(from, to)
  }

  private def readsDeletesSameRelationship(from: Effects, to: Effects) = {
    val fromReadRels = from.effectsSet.collect {
      case readRels: ReadsRelationships => readRels
    }
    fromReadRels.nonEmpty && to.contains(DeletesRelationship)
  }

  private def readsDeletesSameNode(from: Effects, to: Effects) = {
    val fromReadNodes = from.effectsSet.collect {
      case readNodes: ReadsNodes => readNodes
    }
    fromReadNodes.nonEmpty && to.contains(DeletesNode)
  }

  private def readsCreatesSameNodeWithLabels(from: Effects, to: Effects) = {
    val fromLabels = from.effectsSet.collect {
      case ReadsNodesWithLabels(labels) => labels
    }.flatten

    val toLabels = to.effectsSet.collect {
      case CreatesNodesWithLabels(labels) => labels
      case SetLabel(label) => Set(label)
    }.flatten

    (fromLabels intersect toLabels).nonEmpty
  }

  private def nodePropertiesConflict(from: Effects, to: Effects): Boolean = {
    val propertyReads = from.effectsSet.collect {
      case property: ReadsNodeProperty => property
    }

    val propertyWrites = to.effectsSet.collect {
      case property: SetNodeProperty => property
    }

    propertyReads.exists {
      case ReadsAnyNodeProperty => propertyWrites.nonEmpty
      case ReadsGivenNodeProperty(prop) => propertyWrites(SetGivenNodeProperty(prop))
    } ||
      propertyWrites.exists {
        case SetAnyNodeProperty => propertyReads.nonEmpty
        case SetGivenNodeProperty(prop) => propertyReads(ReadsGivenNodeProperty(prop))
      }
  }

  private def relationshipPropertiesConflict(from: Effects, to: Effects): Boolean = {
    val propertyReads = from.effectsSet.collect {
      case property: ReadsRelationshipProperty => property
    }

    val propertyWrites = to.effectsSet.collect {
      case property: WritesRelationshipProperty => property
    }

    propertyReads.exists {
      case ReadsAnyRelationshipProperty => propertyWrites.nonEmpty
      case ReadsGivenRelationshipProperty(prop) => propertyWrites(WritesGivenRelationshipProperty(prop))
    } ||
      propertyWrites.exists {
        case WritesAnyRelationshipProperty => propertyReads.nonEmpty
        case WritesGivenRelationshipProperty(prop) => propertyReads(ReadsGivenRelationshipProperty(prop))
      }
  }
}
