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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EagerPipe, Pipe}

object addEagernessIfNecessary extends (Pipe => Pipe) {

  def apply(toPipe: Pipe): Pipe = {
    val sources = toPipe.sources.map(apply).map { fromPipe =>
      val from = fromPipe.effects
      val to = toPipe.localEffects
      if (wouldInterfere(from, to)) {
        new EagerPipe(fromPipe)(fromPipe.monitor)
      } else {
        fromPipe
      }
    }
    toPipe.dup(sources.toList)
  }

  private def wouldInterfere(fromWithLeafInfo: Effects, toWithLeafInfo: Effects): Boolean = {
//    val nodesInterfere = nodesReadInterference(from, to)
//    val relsInterfere = from.contains(ReadsRelationships) && to.contains(WritesRelationships)

    // NOTE: Leaf effects will not be considered unless effects have first been flattened with Effects.regardlessOfLeafEffects
    val from = fromWithLeafInfo.regardlessOfLeafEffects
    val to = toWithLeafInfo.regardlessOfLeafEffects

    val deleteMergeInterfereNodes = nodesDeleteMergeInterference(from, to)
    val deleteMergeInterfereRelationships = relationshipsDeleteMergeInterference(fromWithLeafInfo, toWithLeafInfo)
    val writeReadInterfereNodes = nodesWriteReadInterference(from, to)
    val readWriteNonLeafInterfereNodes = nodesReadCreateInterference(fromWithLeafInfo, toWithLeafInfo) // NOTE: Here we should _not_ consider leaf effects

    deleteMergeInterfereNodes ||
      deleteMergeInterfereRelationships ||
      nodePropertiesInterfere(fromWithLeafInfo, toWithLeafInfo) ||
      relationshipPropertiesInterfere(fromWithLeafInfo, toWithLeafInfo) ||
      writeReadInterfereNodes ||
      readWriteNonLeafInterfereNodes
  }

  private def nodesWriteReadInterference(from: Effects, to: Effects) = {
    // Flip the order to reuse this code:
    readsCreatesSameNode(to, from)
  }

  def nodesReadCreateInterference(from: Effects, to: Effects) = {
    readsCreatesSameNode(from, to)
  }

  private def relationshipsDeleteMergeInterference(from: Effects, to: Effects) = {
    from.contains(DeletesRelationship) && readsCreatesSameRelationship(from, to)
  }
  private def readsCreatesSameRelationship(from: Effects, to: Effects) = {
    from.contains(ReadsRelationshipsWithAnyType) && to.effectsSet.exists {
      case create: CreatesSomeRelationship => true
      case _ => false
    } || readsCreatesSameRelationshipWithType(from, to)
  }

  private def readsCreatesSameRelationshipWithType(from: Effects, to: Effects) = {
    val fromTypes = from.effectsSet.collect {
      case ReadsRelationshipsWithType(typ) => typ
    }

    val toTypes = to.effectsSet.collect {
      case CreatesRelationship(typ) => typ
    }

    (fromTypes intersect toTypes).nonEmpty
  }

  private def nodesDeleteMergeInterference(from: Effects, to: Effects) = {
    from.contains(DeletesNode) && readsCreatesSameNode(from, to)
  }

  private def readsCreatesSameNode(from: Effects, to: Effects) = {
    from.contains(ReadsAllNodes) && to.contains(CreatesAnyNode) || readsCreatesSameNodeWithLabels(from, to)
  }

  private def readsCreatesSameNodeWithLabels(from: Effects, to: Effects) = {
    val fromLabels = from.effectsSet.collect {
      case ReadsNodesWithLabels(labels) => labels
    }.flatten

    val toLabels = to.effectsSet.collect {
      case CreatesNodesWithLabels(labels) => labels
    }.flatten

    (fromLabels intersect toLabels).nonEmpty
  }

  private def nodePropertiesInterfere(from: Effects, to: Effects): Boolean = {
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

  private def relationshipPropertiesInterfere(from: Effects, to: Effects): Boolean = {
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
