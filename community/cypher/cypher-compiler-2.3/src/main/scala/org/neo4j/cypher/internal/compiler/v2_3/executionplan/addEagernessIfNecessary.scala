/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
  private def wouldInterfere(from: Effects, to: Effects): Boolean = {
    val nodesInterfere = nodesReadInterference(from, to)
    val relsInterfere = from.contains(ReadsRelationships) && to.contains(WritesRelationships)

    val readWriteInterfereNodes = nodesWriteInterference(from, to)
    val readWriteInterfereRelationships = relsWriteInterference(from, to)

    nodesInterfere || relsInterfere || readWriteInterfereNodes || readWriteInterfereRelationships ||
      nodePropertiesInterfere(from, to) || relationshipPropertiesInterfere(from, to)
  }

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

  private def relsWriteInterference(from: Effects, to: Effects) = {
    from.contains(WritesRelationships) && to.contains(WritesRelationships) && to.contains(ReadsRelationships) ||
    from.contains(DeletesRelationship) && to.contains(ReadsRelationships)
  }

  private def nodesWriteInterference(from: Effects, to: Effects) = {
    val fromWrites = from.effectsSet.collect {
      case writes: WritesNodes => writes
    }

    val toReads = to.effectsSet.collect {
      case reads: ReadsNodes => reads
    }
    val toWrites = to.effectsSet.collect {
      case writes: WritesNodes => writes
    }
    val fromDeletes = from.effectsSet.collect {
      case deletes@DeletesNode => deletes
    }
    (fromWrites.contains(WritesAnyNode) && toWrites.nonEmpty && toReads.nonEmpty) ||
    (fromDeletes.nonEmpty && toReads.nonEmpty) ||
    (fromWrites.nonEmpty && toWrites.nonEmpty && toReads.contains(ReadsAllNodes)) ||
      (nodeLabelOverlap(toReads, fromWrites) && toWrites.nonEmpty)
  }

  private def nodesReadInterference(from: Effects, to: Effects) = {
    val fromReads = from.effectsSet.collect {
      case reads: ReadsNodes => reads
    }

    val toWrites = to.effectsSet.collect {
      case writes: WritesNodes => writes
    }

    (fromReads.nonEmpty && toWrites.contains(WritesAnyNode)) ||
      nodeLabelOverlap(fromReads, toWrites)
  }

  private def nodeLabelOverlap(reads: Set[ReadsNodes], writes: Set[WritesNodes]) = {
    val fromLabels = reads.collect {
      case ReadsNodesWithLabels(labels) => labels
    }.flatten

    val toLabels = writes.collect {
      case WritesNodesWithLabels(labels) => labels
    }.flatten

    (fromLabels intersect toLabels).nonEmpty
  }

  private def nodePropertiesInterfere(from: Effects, to: Effects): Boolean = {
    val propertyReads = from.effectsSet.collect {
      case property: ReadsNodeProperty => property
    }

    val propertyWrites = to.effectsSet.collect {
      case property: WritesNodeProperty => property
    }

    propertyReads.exists {
      case ReadsAnyNodeProperty => propertyWrites.nonEmpty
      case ReadsGivenNodeProperty(prop) => propertyWrites(WritesGivenNodeProperty(prop))
    } ||
      propertyWrites.exists {
        case WritesAnyNodeProperty => propertyReads.nonEmpty
        case WritesGivenNodeProperty(prop) => propertyReads(ReadsGivenNodeProperty(prop))
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
