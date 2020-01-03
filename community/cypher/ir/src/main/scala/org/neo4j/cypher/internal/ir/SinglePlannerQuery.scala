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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.v4_0.ast.Hint
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, Variable}
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3

/**
  * A linked list of queries, each made up of, a query graph (MATCH ... WHERE ...), a required order, a horizon (WITH ...) and a pointer to the next query.
  */
trait SinglePlannerQuery extends PlannerQueryPart {

  /**
   * Optionally, an input to the query provided using INPUT DATA STREAM. These are the column names provided by IDS.
   */
  val queryInput: Option[Seq[String]]
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
  val tail: Option[SinglePlannerQuery]

  def dependencies: Set[String]

  override def readOnly: Boolean = (queryGraph.readOnly && horizon.readOnly) && tail.forall(_.readOnly)

  def preferredStrictness: Option[StrictnessMode] =
    horizon.preferredStrictness(interestingOrder.requiredOrderCandidate.nonEmpty) orElse tail.flatMap(_.preferredStrictness)

  def last: SinglePlannerQuery = tail.map(_.last).getOrElse(this)

  def lastQueryGraph: QueryGraph = last.queryGraph
  def lastQueryHorizon: QueryHorizon = last.horizon

  def withTail(newTail: SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None => copy(tail = Some(newTail))
    case Some(_) => throw new InternalException("Attempt to set a second tail on a query graph")
  }

  def withInput(queryInput: Seq[String]): SinglePlannerQuery =
    copy(input = Some(queryInput), queryGraph = queryGraph.copy(argumentIds = queryGraph.argumentIds ++ queryInput))

  override def withoutHints(hintsToIgnore: Set[Hint]): SinglePlannerQuery = {
    copy(queryGraph = queryGraph.withoutHints(hintsToIgnore), tail = tail.map(x => x.withoutHints(hintsToIgnore)))
  }

  def withHorizon(horizon: QueryHorizon): SinglePlannerQuery = copy(horizon = horizon)

  def withQueryGraph(queryGraph: QueryGraph): SinglePlannerQuery = copy(queryGraph = queryGraph)

  def withInterestingOrder(interestingOrder: InterestingOrder): SinglePlannerQuery =
    copy(interestingOrder = interestingOrder)

  def withTailInterestingOrder(interestingOrder: InterestingOrder): SinglePlannerQuery = {
    def f(plannerQuery: SinglePlannerQuery): (SinglePlannerQuery, InterestingOrder) = {
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

  def isCoveredByHints(other: SinglePlannerQuery): Boolean = allHints.forall(other.allHints.contains)

  override def allHints: Set[Hint] = tail match {
    case Some(tailPlannerQuery) => queryGraph.allHints ++ tailPlannerQuery.allHints
    case None => queryGraph.allHints
  }

  override def numHints: Int = allHints.size

  def amendQueryGraph(f: QueryGraph => QueryGraph): SinglePlannerQuery = withQueryGraph(f(queryGraph))

  def updateHorizon(f: QueryHorizon => QueryHorizon): SinglePlannerQuery = withHorizon(f(horizon))

  def updateQueryProjection(f: QueryProjection => QueryProjection): SinglePlannerQuery = horizon match {
    case projection: QueryProjection => withHorizon(f(projection))
    case _ => throw new InternalException("Tried updating projection when there was no projection there")
  }

  def updateTail(f: SinglePlannerQuery => SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None => this
    case Some(tailQuery) => copy(tail = Some(f(tailQuery)))
  }

  def updateTailOrSelf(f: SinglePlannerQuery => SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None => f(this)
    case Some(_) => this.updateTail(_.updateTailOrSelf(f))
  }

  def tailOrSelf: SinglePlannerQuery = tail match {
    case None => this
    case Some(t) => t.tailOrSelf
  }

  def exists(f: SinglePlannerQuery => Boolean): Boolean =
    f(this) || tail.exists(_.exists(f))

  def ++(other: SinglePlannerQuery): SinglePlannerQuery = {
    (this.horizon, other.horizon) match {
      case (a: RegularQueryProjection, b: RegularQueryProjection) =>
        RegularSinglePlannerQuery(
          queryGraph = queryGraph ++ other.queryGraph,
          interestingOrder = interestingOrder,
          horizon = a ++ b,
          tail = either(tail, other.tail),
          queryInput = either(queryInput, other.queryInput))

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
                     tail: Option[SinglePlannerQuery] = tail,
                     input: Option[Seq[String]] = queryInput): SinglePlannerQuery

  def foldMap(f: (SinglePlannerQuery, SinglePlannerQuery) => SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None => this
    case Some(oldTail) =>
      val newTail = f(this, oldTail)
      copy(tail = Some(newTail.foldMap(f)))
  }

  def fold[A](in: A)(f: (A, SinglePlannerQuery) => A): A = {

    @tailrec
    def recurse(acc: A, pq: SinglePlannerQuery): A = {
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
  def allPlannerQueries: Seq[SinglePlannerQuery] = {
    @tailrec
    def loop(acc: Seq[SinglePlannerQuery], remaining: Option[SinglePlannerQuery]): Seq[SinglePlannerQuery] = remaining match {
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

  override def returns: Set[String] = {
    lastQueryHorizon match {
      case projection: QueryProjection => projection.keySet
      case _ => Set.empty
    }
  }

  override def asSinglePlannerQuery: SinglePlannerQuery = this
}

object SinglePlannerQuery {
  val empty = RegularSinglePlannerQuery()

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]): Set[String] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }
}

case class RegularSinglePlannerQuery(queryGraph: QueryGraph = QueryGraph.empty,
                                     interestingOrder: InterestingOrder = InterestingOrder.empty,
                                     horizon: QueryHorizon = QueryProjection.empty,
                                     tail: Option[SinglePlannerQuery] = None,
                                     queryInput: Option[Seq[String]] = None) extends SinglePlannerQuery {

  // This is here to stop usage of copy from the outside
  override protected def copy(queryGraph: QueryGraph = queryGraph,
                              interestingOrder: InterestingOrder = interestingOrder,
                              horizon: QueryHorizon = horizon,
                              tail: Option[SinglePlannerQuery] = tail,
                              queryInput: Option[Seq[String]] = queryInput): SinglePlannerQuery =
    RegularSinglePlannerQuery(queryGraph, interestingOrder, horizon, tail, queryInput)

  override def dependencies: Set[String] = horizon.dependencies ++ queryGraph.dependencies ++ tail.map(_.dependencies).getOrElse(Set.empty)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RegularSinglePlannerQuery]

  override def equals(other: Any): Boolean = other match {
    case that: RegularSinglePlannerQuery =>
      (that canEqual this) &&
        queryInput == that.queryInput &&
        queryGraph == that.queryGraph &&
        horizon == that.horizon &&
        tail == that.tail &&
        interestingOrder.requiredOrderCandidate.order == that.interestingOrder.requiredOrderCandidate.order
    case _ => false
  }

  var theHashCode: Int = -1

  override def hashCode(): Int = {
    if (theHashCode == -1) {
      val state = Seq(queryInput, queryGraph, horizon, tail, interestingOrder.requiredOrderCandidate.order)
      theHashCode = MurmurHash3.seqHash(state)
    }
    theHashCode
  }
}
