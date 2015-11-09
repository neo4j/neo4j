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
package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, PatternRelationship, StrictnessMode}
import org.neo4j.cypher.internal.frontend.v3_0.InternalException
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Hint, LabelName}
import org.neo4j.cypher.internal.frontend.v3_0.perty._

import scala.annotation.tailrec
import scala.collection.GenTraversableOnce

case class UnionQuery(queries: Seq[PlannerQuery], distinct: Boolean, returns: Seq[IdName])

case class PlannerQuery(queryGraph: QueryGraph = QueryGraph.empty,
                        updateGraph: UpdateGraph = UpdateGraph.empty,
                        horizon: QueryHorizon = QueryProjection.empty,
                        tail: Option[PlannerQuery] = None)
  extends PageDocFormatting {
  // with ToPrettyString[PlannerQuery] {

  //  def toDefaultPrettyString(formatter: DocFormatter) =
  //    toPrettyString(formatter)(InternalDocHandler.docGen)

  def preferredStrictness: Option[StrictnessMode] =
    horizon.preferredStrictness orElse tail.flatMap(_.preferredStrictness)

  def last: PlannerQuery = tail.map(_.last).getOrElse(this)

  def lastQueryGraph: QueryGraph = last.queryGraph
  def lastUpdateGraph: UpdateGraph = last.updateGraph
  def lastQueryHorizon: QueryHorizon = last.horizon

  def withTail(newTail: PlannerQuery): PlannerQuery = tail match {
    case None => copy(tail = Some(newTail))
    case Some(_) => throw new InternalException("Attempt to set a second tail on a query graph")
  }

  def readOnly: Boolean = updateGraph.isEmpty && queryGraph.nonEmpty

  def writeOnly: Boolean = queryGraph.isEmpty && updateGraph.nonEmpty

  def writes: Boolean = exists(_.updateGraph.nonEmpty)

  def createsNodes: Boolean = exists(_.updateGraph.createNodePatterns.nonEmpty)

  def updatesNodes: Boolean = exists(_.updateGraph.updatesNodes)

  def writesRelationships: Boolean = exists(_.updateGraph.createRelationshipPatterns.nonEmpty)

  def withoutHints(hintsToIgnore: GenTraversableOnce[Hint]) = copy(queryGraph = queryGraph.withoutHints(hintsToIgnore))

  def withHorizon(horizon: QueryHorizon): PlannerQuery = copy(horizon = horizon)

  def withQueryGraph(queryGraph: QueryGraph): PlannerQuery = copy(queryGraph = queryGraph)

  def withUpdateGraph(updateGraph: UpdateGraph) = copy(updateGraph = updateGraph)

  def isCoveredByHints(other: PlannerQuery) = allHints.forall(other.allHints.contains)

  def allHints: Set[Hint] = tail match {
    case Some(tailPlannerQuery) => queryGraph.allHints ++ tailPlannerQuery.allHints
    case None => queryGraph.allHints
  }

  def numHints: Int = tail match {
    case Some(tailPlannerQuery) => queryGraph.numHints + tailPlannerQuery.numHints
    case None => queryGraph.numHints
  }

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQuery = withQueryGraph(f(queryGraph))

  def amendUpdateGraph(f: UpdateGraph => UpdateGraph): PlannerQuery = withUpdateGraph(f(updateGraph))

  def updateHorizon(f: QueryHorizon => QueryHorizon): PlannerQuery = withHorizon(f(horizon))

  def updateQueryProjection(f: QueryProjection => QueryProjection): PlannerQuery = horizon match {
    case projection: QueryProjection => withHorizon(f(projection))
    case _ => throw new InternalException("Tried updating projection when there was no projection there")
  }

  def updateTail(f: PlannerQuery => PlannerQuery) = tail match {
    case None => this
    case Some(tailQuery) => copy(tail = Some(f(tailQuery)))
  }

  def updateTailOrSelf(f: PlannerQuery => PlannerQuery): PlannerQuery = tail match {
    case None => f(this)
    case Some(tailQuery) => this.updateTail(_.updateTailOrSelf(f))
  }

  def exists(f: PlannerQuery => Boolean): Boolean =
    f(this) || tail.exists(_.exists(f))

  def ++(other: PlannerQuery): PlannerQuery = {
    (this.horizon, other.horizon) match {
      case (a: RegularQueryProjection, b: RegularQueryProjection) =>
        PlannerQuery(
          horizon = a ++ b,
          queryGraph = queryGraph ++ other.queryGraph,
          updateGraph = updateGraph ++ other.updateGraph,
          tail = either(tail, other.tail)
        )

      case _ =>
        throw new InternalException("Tried to concatenate non-regular query projections")
    }
  }

  private def either[T](a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_)) => throw new InternalException("Can't join two query graphs with different SKIP")
    case (s@Some(_), None) => s
    case (None, s) => s
  }

  // This is here to stop usage of copy from the outside
  private def copy(queryGraph: QueryGraph = queryGraph,
                   updateGraph: UpdateGraph = updateGraph,
                   horizon: QueryHorizon = horizon,
                   tail: Option[PlannerQuery] = tail) = PlannerQuery(queryGraph, updateGraph, horizon, tail)

  def foldMap(f: (PlannerQuery, PlannerQuery) => PlannerQuery): PlannerQuery = tail match {
    case None => this
    case Some(oldTail) =>
      val newTail = f(this, oldTail)
      copy(tail = Some(newTail.foldMap(f)))
  }

  def fold[A](in: A)(f: (A, PlannerQuery) => A): A = {

    @tailrec
    def recurse(acc: A, pq: PlannerQuery): A = {
      val nextAcc = f(acc, pq)

      pq.tail match {
        case Some(tailPQ) => recurse(nextAcc, tailPQ)
        case None => nextAcc
      }
    }

    recurse(in, this)
  }

  //Returns list of querygraph of planner query and all of its tails
  def allQueryGraphs: Seq[QueryGraph] = allPlannerQueries.map(_.queryGraph)


  //Returns list of planner query and all of its tails
  def allPlannerQueries: Seq[PlannerQuery] = {
    @tailrec
    def loop(acc: Seq[PlannerQuery], remaining: Option[PlannerQuery]): Seq[PlannerQuery] = remaining match {
      case None => acc
      case Some(inner) => loop(acc :+ inner, inner.tail)
    }

    loop(Seq.empty, Some(this))
  }

  def labelInfo: Map[IdName, Set[LabelName]] = lastQueryGraph.selections.labelInfo
}

object PlannerQuery {
  val empty = PlannerQuery()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }
}

trait CardinalityEstimation {
  self: PlannerQuery =>

  def estimatedCardinality: Cardinality
}

object CardinalityEstimation {
  def lift(plannerQuery: PlannerQuery, cardinality: Cardinality) =
    new PlannerQuery(plannerQuery.queryGraph, plannerQuery.updateGraph, plannerQuery.horizon, plannerQuery.tail) with CardinalityEstimation {
      val estimatedCardinality = cardinality
    }
}
