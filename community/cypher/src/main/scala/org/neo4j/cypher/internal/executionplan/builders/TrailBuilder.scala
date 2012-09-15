/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.graphdb.{Direction, PropertyContainer}
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.internal.commands.{Predicate, RelatedTo, True}


object TrailBuilder {
  def findLongestTrail(patterns: Seq[RelatedTo], boundPoints: Seq[String], predicates: Seq[Predicate] = Seq.empty) =
    new TrailBuilder(patterns, boundPoints, predicates).findLongestTrail()
}

final case class LongestTrail(start: String, end: Option[String], longestTrail: Trail) {
  lazy val step = longestTrail.toSteps(0).get.reverse()
}

final class TrailBuilder(patterns: Seq[RelatedTo], boundPoints: Seq[String], predicates: Seq[Predicate]) {
  private def internalFindLongestPath(doneSeq: Seq[(Trail, Seq[RelatedTo])]): Seq[(Trail, Seq[RelatedTo])] = {
    val result: Seq[(Trail, Seq[RelatedTo])] = doneSeq.flatMap {
      case (done: Trail, patterns: Seq[RelatedTo]) =>
        val relatedToes = patterns.filter {
          rel => done.end == rel.left || done.end == rel.right
        }

        if (relatedToes.isEmpty)
          Seq((done, patterns))
        else {
          Seq((done, patterns)) ++
          relatedToes.map {
            case rel if rel.left == done.end => (WrappingTrail(done, rel.direction.reverse(), rel.relName, rel.relTypes, rel.right, predicates, rel), patterns.filterNot(_ == rel))
            case rel                         => (WrappingTrail(done, rel.direction, rel.relName, rel.relTypes, rel.left, predicates, rel), patterns.filterNot(_ == rel))
          }
        }
    }

    if (result.distinct == doneSeq.distinct)
      result
    else
      internalFindLongestPath(result)
  }

  private def findLongestTrail(): Option[LongestTrail] =
    if (patterns.isEmpty) {
      None
    }
    else {
      val foundPaths: Seq[(Trail, Seq[RelatedTo])] = findAllPaths()
      val pathsBetweenBoundPoints: Seq[(Trail, Seq[RelatedTo])] = findCompatiblePaths(foundPaths)

      if (pathsBetweenBoundPoints.isEmpty) {
        None
      } else {
        val trail = findLongestTrail(pathsBetweenBoundPoints)

        Some(trail)
      }
    }


  private def findLongestTrail(pathsBetweenBoundPoints: scala.Seq[(Trail, scala.Seq[RelatedTo])]): LongestTrail = {
    val almost = pathsBetweenBoundPoints.sortBy(_._1.size)
    val (longestPath, _) = almost.last

    val start = longestPath.start
    val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None
    val trail = LongestTrail(start, end, longestPath)
    trail
  }

  private def findAllPaths(): Seq[(Trail, scala.Seq[RelatedTo])] = {
    val startPoints = boundPoints.map(point => (BoundPoint(point), patterns))
    val foundPaths = internalFindLongestPath(startPoints).
      filter {
      case (trail, toes) => trail.size > 0 && trail.start != trail.end
    }
    foundPaths
  }

  private def findCompatiblePaths(incomingPaths: Seq[(Trail, Seq[RelatedTo])]): Seq[(Trail, Seq[RelatedTo])] = {
    val foundPaths = incomingPaths.filterNot {
      case (trail, _) => hasBoundPointsInMiddleOfPath(trail)
    }


    val boundInTwoPoints = foundPaths.filter {
      case (p, left) => boundPoints.contains(p.start) && boundPoints.contains(p.end)
    }

    val boundInAtLeastOnePoints = foundPaths.filter {
      case (p, left) => boundPoints.contains(p.start) || boundPoints.contains(p.end)
    }

    if (boundInTwoPoints.nonEmpty)
      boundInTwoPoints
    else
      boundInAtLeastOnePoints
  }

  def hasBoundPointsInMiddleOfPath(trail: Trail): Boolean = {
    trail.pathDescription.slice(1, trail.pathDescription.size - 1).exists(boundPoints.contains)
  }
}