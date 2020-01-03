/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.v3_5

import org.neo4j.cypher.internal.v3_5.ast.Hint
import org.neo4j.cypher.internal.v3_5.expressions.LabelName
import org.neo4j.cypher.internal.v3_5.expressions.Variable
import org.neo4j.cypher.internal.v3_5.util.InternalException

import scala.annotation.tailrec
import scala.collection.GenSeq
import scala.util.hashing.MurmurHash3

/**
  * A linked list of queries, each made up of, a query graph (MATCH ... WHERE ...), a required order, a horizon (WITH ...) and a pointer to the next query.
  */
trait PlannerQuery {
  /**
    * The part of query from a MATCH/MERGE/CREATE until (excluding) the next WITH/RETURN.
    */
  val queryGraph: QueryGraph
  /**
    * The required order of a query graph and its horizon. The required order emerges from an ORDER BY or aggregation or distinct.
    */
  val interestingOrder: InterestingOrder
  /**
    * The WITH/RETURN part of a query
    */
  val horizon: QueryHorizon
  /**
    * Optionally, a next PlannerQuery for everything after the WITH in the current horizon.
    */
  val tail: Option[PlannerQuery]

  def dependencies: Set[String]

  def readOnly: Boolean = (queryGraph.readOnly && horizon.readOnly) && tail.forall(_.readOnly)

  def preferredStrictness: Option[StrictnessMode] =
    horizon.preferredStrictness orElse tail.flatMap(_.preferredStrictness)

  def last: PlannerQuery = tail.map(_.last).getOrElse(this)

  def lastQueryGraph: QueryGraph = last.queryGraph
  def lastQueryHorizon: QueryHorizon = last.horizon

  def withTail(newTail: PlannerQuery): PlannerQuery = tail match {
    case None => copy(tail = Some(newTail))
    case Some(_) => throw new InternalException("Attempt to set a second tail on a query graph")
  }

  def withoutHints(hintsToIgnore: GenSeq[Hint]): PlannerQuery = {
    copy(queryGraph = queryGraph.withoutHints(hintsToIgnore), tail = tail.map((x) => x.withoutHints(hintsToIgnore)))
  }

  def withHorizon(horizon: QueryHorizon): PlannerQuery = copy(horizon = horizon)

  def withQueryGraph(queryGraph: QueryGraph): PlannerQuery = copy(queryGraph = queryGraph)

  def withInterestingOrder(interestingOrder: InterestingOrder): PlannerQuery =
    copy(interestingOrder = interestingOrder)

  def withTailInterestingOrder(interestingOrder: InterestingOrder): PlannerQuery = {
    def f(plannerQuery: PlannerQuery): (PlannerQuery, InterestingOrder) = {
      plannerQuery.tail match {
        case None => (plannerQuery.copy(interestingOrder = interestingOrder), interestingOrder.asInteresting)
        case Some(q) =>
          val (newTail, tailOrder) = f(q)
          if (plannerQuery.interestingOrder.isEmpty) {
            val reverseProjected =
              plannerQuery.horizon match {
                case qp: QueryProjection => tailOrder.withReverseProjectedColumns(qp.projections, newTail.queryGraph.argumentIds)
                case _ => tailOrder
              }
            (plannerQuery.copy(interestingOrder = reverseProjected, tail = Some(newTail)), reverseProjected)
          } else
            (plannerQuery.copy(tail = Some(newTail)), InterestingOrder.empty)
      }
    }

    f(this)._1
  }

  def isCoveredByHints(other: PlannerQuery) = allHints.forall(other.allHints.contains)

  def allHints: Seq[Hint] = tail match {
    case Some(tailPlannerQuery) => queryGraph.allHints ++ tailPlannerQuery.allHints
    case None => queryGraph.allHints
  }

  def numHints: Int = allHints.size

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQuery = withQueryGraph(f(queryGraph))

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

  def tailOrSelf: PlannerQuery = tail match {
    case None => this
    case Some(t) => t.tailOrSelf
  }

  def exists(f: PlannerQuery => Boolean): Boolean =
    f(this) || tail.exists(_.exists(f))

  def ++(other: PlannerQuery): PlannerQuery = {
    (this.horizon, other.horizon) match {
      case (a: RegularQueryProjection, b: RegularQueryProjection) =>
        RegularPlannerQuery(
          horizon = a ++ b,
          queryGraph = queryGraph ++ other.queryGraph,
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
  protected def copy(queryGraph: QueryGraph = queryGraph,
                     interestingOrder: InterestingOrder = interestingOrder,
                     horizon: QueryHorizon = horizon,
                     tail: Option[PlannerQuery] = tail): PlannerQuery

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

  //Returns a list of query graphs from this plannerquery and all of its tails
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

  def labelInfo: Map[String, Set[LabelName]] = {
    val labelInfo = lastQueryGraph.selections.labelInfo
    val projectedLabelInfo = lastQueryHorizon match {
      case projection: QueryProjection =>
        projection.projections.collect {
          case (projectedName, Variable(name)) if labelInfo.contains(name) =>
              projectedName -> labelInfo(name)
        }
      case _ => Map.empty[String, Set[LabelName]]
    }
    labelInfo ++ projectedLabelInfo
  }
}

object PlannerQuery {
  val empty = RegularPlannerQuery()

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }
}

case class RegularPlannerQuery(queryGraph: QueryGraph = QueryGraph.empty,
                               interestingOrder: InterestingOrder = InterestingOrder.empty,
                               horizon: QueryHorizon = QueryProjection.empty,
                               tail: Option[PlannerQuery] = None) extends PlannerQuery {

  // This is here to stop usage of copy from the outside
  override protected def copy(queryGraph: QueryGraph = queryGraph,
                              interestingOrder: InterestingOrder = interestingOrder,
                              horizon: QueryHorizon = horizon,
                              tail: Option[PlannerQuery] = tail) =
    RegularPlannerQuery(queryGraph, interestingOrder, horizon, tail)

  override def dependencies: Set[String] = horizon.dependencies ++ queryGraph.dependencies ++ tail.map(_.dependencies).getOrElse(Set.empty)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RegularPlannerQuery]

  override def equals(other: Any): Boolean = other match {
    case that: RegularPlannerQuery =>
      (that canEqual this) &&
        queryGraph == that.queryGraph &&
        horizon == that.horizon &&
        tail == that.tail &&
        interestingOrder.required == that.interestingOrder.required
    case _ => false
  }

  var theHashCode: Int = -1

  override def hashCode(): Int = {
    if (theHashCode == -1) {
      val state = Seq(queryGraph, horizon, tail, interestingOrder.required)
      theHashCode = MurmurHash3.seqHash(state)
    }
    theHashCode
  }
}

case class UnionQuery(queries: Seq[PlannerQuery], distinct: Boolean, returns: Seq[String], periodicCommit: Option[PeriodicCommit]) {
  def readOnly: Boolean = queries.forall(_.readOnly)
}
