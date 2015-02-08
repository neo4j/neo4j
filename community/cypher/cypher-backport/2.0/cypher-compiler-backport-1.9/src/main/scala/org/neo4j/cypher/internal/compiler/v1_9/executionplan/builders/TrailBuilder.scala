/**
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.VariableLengthStepTrail
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.EndPoint
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.RelationshipIdentifier
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.SingleStepTrail
import annotation.tailrec

object TrailBuilder {
  def findLongestTrail(patterns: Seq[Pattern], boundPoints: Seq[String], predicates: Seq[Predicate] = Nil) =
    new TrailBuilder(patterns, boundPoints, predicates).findLongestTrail()
}

final case class LongestTrail(start: String, end: Option[String], longestTrail: Trail) {
  lazy val step = longestTrail.toSteps(0).get
}

final class TrailBuilder(patterns: Seq[Pattern], boundPoints: Seq[String], predicates: Seq[Predicate]) {
  @tailrec
  private def internalFindLongestPath(doneSeq: Seq[(Trail, Seq[Pattern])]): Seq[(Trail, Seq[Pattern])] = {

    def createFinder(elem: String): (Predicate => Boolean) = {
      def containsSingle(set: Set[String]) = set.size == 1 && set.head == elem
      (pred: Predicate) => containsSingle(pred.symbolTableDependencies)
    }

    def transformToTrail(p: Pattern, done: Trail, patternsToDo: Seq[Pattern]): (Trail, Seq[Pattern]) = {

      def rewriteTo(originalName: String, newExpr: Expression)(e: Expression) = e match {
        case Identifier(name) if name == originalName => newExpr
        case _                                        => e
      }

      def findPredicates(k: String): Seq[Predicate] = predicates.filter(createFinder(k))

      def singleStep(rel: RelatedTo, end: String, dir: Direction) = {
        val orgRelPred: Seq[Predicate] = findPredicates(rel.relName)
        val orgNodePred: Seq[Predicate] = findPredicates(end)

        val relPred: Predicate = Predicate.
          fromSeq(orgRelPred).
          rewrite(rewriteTo(rel.relName, RelationshipIdentifier()))

        val nodePred: Predicate = Predicate.
          fromSeq(orgNodePred).
          rewrite(rewriteTo(end, NodeIdentifier()))

        done.add(start => SingleStepTrail(EndPoint(end), dir, rel.relName, rel.relTypes, start, relPred, nodePred, rel, orgNodePred ++ orgRelPred))
      }

      def multiStep(rel: VarLengthRelatedTo, end: String, dir: Direction) =
        done.add(start => VariableLengthStepTrail(EndPoint(end), dir, rel.relTypes, rel.minHops.getOrElse(1), rel.maxHops, rel.pathName, rel.relIterator, start, rel))

      val patternsLeft = patternsToDo.filterNot(_ == p)

      val result: Trail = p match {
        case rel: RelatedTo if rel.left == done.end           => singleStep(rel, rel.right, rel.direction)
        case rel: RelatedTo if rel.right == done.end          => singleStep(rel, rel.left, rel.direction.reverse())
        case rel: VarLengthRelatedTo if rel.end == done.end   => multiStep(rel, rel.start, rel.direction.reverse())
        case rel: VarLengthRelatedTo if rel.start == done.end => multiStep(rel, rel.end, rel.direction)
        case _                                                => throw new ThisShouldNotHappenError("Andres", "This pattern is not expected")
      }

      (result, patternsLeft)
    }

    val result: Seq[(Trail, Seq[Pattern])] = doneSeq.flatMap {
      case (done: Trail, patternsToDo: Seq[Pattern]) =>
        val relatedToes: Seq[Pattern] = patternsToDo.filter {
          case rel: RelatedTo          => done.end == rel.left || done.end == rel.right
          case rel: VarLengthRelatedTo => done.end == rel.end || done.end == rel.start
        }

        val newValues = relatedToes.map(transformToTrail(_, done, patternsToDo))
        Seq((done, patternsToDo)) ++ newValues
    }

    val uniqueResults = result.distinct
    val uniqueInput = doneSeq.distinct

    if (uniqueResults == uniqueInput)
      result
    else
      internalFindLongestPath(uniqueResults)
  }

  private def findLongestTrail(): Option[LongestTrail] = {
    def findAllPaths(): Seq[(Trail, scala.Seq[Pattern])] = {
      val startPoints = boundPoints.map(point => (EndPoint(point), patterns))
      val foundPaths = internalFindLongestPath(startPoints)
      val filteredPaths = foundPaths.filter {
        case (trail, toes) => !trail.isEndPoint && trail.start != trail.end
      }
      filteredPaths
    }


    if (patterns.isEmpty) {
      None
    }
    else {
      val foundPaths: Seq[(Trail, Seq[Pattern])] = findAllPaths()
      val pathsBetweenBoundPoints: Seq[(Trail, Seq[Pattern])] = findCompatiblePaths(foundPaths)

      if (pathsBetweenBoundPoints.isEmpty) {
        None
      } else {
        val trail = findLongestTrail(pathsBetweenBoundPoints)

        Some(trail)
      }
    }
  }

  private def findLongestTrail(pathsBetweenBoundPoints: scala.Seq[(Trail, scala.Seq[Pattern])]): LongestTrail = {
    val almost = pathsBetweenBoundPoints.sortWith {
      case ((t1, _), (t2, _)) => t1.size < t2.size || t1.start > t2.start //Sort first by length, and then by start point
    }

    val (longestPath, _) = almost.last

    val start = longestPath.start
    val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None
    LongestTrail(start, end, longestPath)
  }


  private def findCompatiblePaths(incomingPaths: Seq[(Trail, Seq[Pattern])]): Seq[(Trail, Seq[Pattern])] = {
    val pathsWithoutBoundPointsInMiddle = incomingPaths.filterNot {
      case (trail, _) => hasBoundPointsInMiddleOfPath(trail)
    }

    val boundInBothEnds = pathsWithoutBoundPointsInMiddle.filter {
      case (p, _) =>
        val startBound = boundPoints.contains(p.start)
        val endBound = boundPoints.contains(p.end)
        val numberOfVarlength = p.filter(_.isInstanceOf[VariableLengthStepTrail]).size
        val numberOfTrails = p.asSeq.size
        val validVarlength = (numberOfVarlength == 1 && numberOfTrails == 1)
        val noVarlength = numberOfVarlength == 0

        startBound && endBound && (validVarlength || noVarlength)
    }

    val boundInOneEnd = pathsWithoutBoundPointsInMiddle.filter {
      case (p, _) =>
        val startBound = boundPoints.contains(p.start)
        val endBound = boundPoints.contains(p.end)
        val numberOfVarlength = p.filter(_.isInstanceOf[VariableLengthStepTrail]).size
        val seq = p.asSeq
        val idxOfVar = seq.indexWhere(_.isInstanceOf[VariableLengthStepTrail])

        val numberOfTrails = p.asSeq.size - 2
        val result = startBound && !endBound && numberOfVarlength <= 1 && (numberOfVarlength == 0 || idxOfVar == numberOfTrails)
        result
    }

    if (boundInBothEnds.nonEmpty)
      boundInBothEnds
    else
      boundInOneEnd
  }

  def hasBoundPointsInMiddleOfPath(trail: Trail): Boolean = {
    val nodesInBetween = trail.nodeNames.toSet -- Set(trail.start, trail.end)

    nodesInBetween exists (boundPoints.contains)
  }
}
