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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.SinglePlannerQuery.extractLabelInfo
import org.neo4j.cypher.internal.ir.SinglePlannerQuery.reverseProjectedInterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * A query in a representation that is consumed by the planner.
 */
sealed trait PlannerQuery {
  def readOnly: Boolean
  def returns: Set[String]

  def allHints: Set[Hint]
  def withoutHints(hintsToIgnore: Set[Hint]): PlannerQuery
  def numHints: Int
  def visitHints[A](acc: A)(f: (A, Hint, QueryGraph) => A): A

  /**
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  def allQGsWithLeafInfo: collection.Seq[QgWithLeafInfo]

  /**
   * Use this method when you are certain that you are dealing with a SinglePlannerQuery, and not a UnionQuery.
   * It will throw an Exception if this is a UnionQuery.
   */
  def asSinglePlannerQuery: SinglePlannerQuery

  /**
   * Flattens all updates (recursively) inside FOREACH, inside this query.
   *
   * The resulting Query is not semantically equivalent (or even plannable), but useful for Eagerness analysis.
   */
  def flattenForeach: PlannerQuery
}

/**
 * This represents the union of the queries.
 * @param lhs the first part, which can itself be either a UnionQuery or a SinglePlannerQuery
 * @param rhs the second part, which is a SinglePlannerQuery
 * @param distinct whether it is a distinct union
 * @param unionMappings mappings of return items from both parts
 */
case class UnionQuery(
  lhs: PlannerQuery,
  rhs: SinglePlannerQuery,
  distinct: Boolean,
  unionMappings: List[UnionMapping]
) extends PlannerQuery {
  override def readOnly: Boolean = lhs.readOnly && rhs.readOnly

  override def returns: Set[String] = lhs.returns.map { returnColInLhs =>
    unionMappings.collectFirst {
      case UnionMapping(varAfterUnion, varInLhs, _) if varInLhs.name == returnColInLhs => varAfterUnion.name
    }.get
  }

  override def allHints: Set[Hint] = lhs.allHints ++ rhs.allHints

  override def withoutHints(hintsToIgnore: Set[Hint]): PlannerQuery = copy(
    lhs = lhs.withoutHints(hintsToIgnore),
    rhs = rhs.withoutHints(hintsToIgnore)
  )

  override def numHints: Int = lhs.numHints + rhs.numHints

  override def visitHints[A](acc: A)(f: (A, Hint, QueryGraph) => A): A = {
    val queryAcc = rhs.visitHints(acc)(f)
    lhs.visitHints(queryAcc)(f)
  }

  override def asSinglePlannerQuery: SinglePlannerQuery =
    throw new IllegalStateException("Called asSinglePlannerQuery on a UnionQuery")

  override def allQGsWithLeafInfo: collection.Seq[QgWithLeafInfo] = lhs.allQGsWithLeafInfo ++ rhs.allQGsWithLeafInfo

  override def flattenForeach: PlannerQuery = copy(
    lhs = lhs.flattenForeach,
    rhs = rhs.flattenForeach
  )
}

/**
 * A linked list of queries, each made up of, a query graph (MATCH ... WHERE ...), a required order, a horizon (WITH ...) and a pointer to the next query.
 */
sealed trait SinglePlannerQuery extends PlannerQuery {

  /**
   * Optionally, an input to the query provided using INPUT DATA STREAM. These are the column names provided by IDS.
   */
  val queryInput: Option[Seq[Variable]]

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

  def readOnlySelf: Boolean = queryGraph.readOnly && horizon.readOnly
  override def readOnly: Boolean = readOnlySelf && tail.forall(_.readOnly)

  def last: SinglePlannerQuery = tail.map(_.last).getOrElse(this)

  def lastQueryGraph: QueryGraph = last.queryGraph
  def lastQueryHorizon: QueryHorizon = last.horizon

  def withTail(newTail: SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None    => copy(tail = Some(newTail))
    case Some(_) => throw new InternalException("Attempt to set a second tail on a query graph")
  }

  def withoutTail: SinglePlannerQuery = tail match {
    case None => this
    case _    => copy(tail = None)
  }

  def withoutLast: Option[SinglePlannerQuery] = tail match {
    case Some(tt) if tt.tail.isEmpty => Some(copy(tail = None))
    case Some(tt)                    => Some(copy(tail = Some(tt.withoutLast.get)))
    case None                        => None
  }

  def withInput(queryInput: Seq[Variable]): SinglePlannerQuery =
    copy(
      input = Some(queryInput),
      queryGraph = queryGraph.withArgumentIds(queryGraph.argumentIds ++ queryInput.map(_.name))
    )

  override def withoutHints(hintsToIgnore: Set[Hint]): SinglePlannerQuery = {
    copy(
      queryGraph = queryGraph.removeHints(hintsToIgnore),
      horizon = horizon.withoutHints(hintsToIgnore),
      tail = tail.map(x => x.withoutHints(hintsToIgnore))
    )
  }

  def withHorizon(horizon: QueryHorizon): SinglePlannerQuery = copy(horizon = horizon)

  def withQueryGraph(queryGraph: QueryGraph): SinglePlannerQuery = copy(queryGraph = queryGraph)

  def withInterestingOrder(interestingOrder: InterestingOrder): SinglePlannerQuery =
    copy(interestingOrder = interestingOrder)

  /**
   * Sets an interestingOrder on the last part of this query and also propagates it to previous query parts.
   */
  def withTailInterestingOrder(interestingOrder: InterestingOrder): SinglePlannerQuery = {
    def f(plannerQuery: SinglePlannerQuery): (SinglePlannerQuery, InterestingOrder) = {
      plannerQuery.tail match {
        case None => (plannerQuery.copy(interestingOrder = interestingOrder), interestingOrder.asInteresting)
        case Some(q) =>
          val (newTail, tailOrder) = f(q)
          if (plannerQuery.interestingOrder.isEmpty) {
            val reverseProjected =
              reverseProjectedInterestingOrder(tailOrder, plannerQuery.horizon, newTail.queryGraph.argumentIds)
            (plannerQuery.copy(interestingOrder = reverseProjected, tail = Some(newTail)), reverseProjected)
          } else
            (plannerQuery.copy(tail = Some(newTail)), InterestingOrder.empty)
      }
    }

    f(this)._1
  }

  /**
   * First interesting order with non-empty required order that is usable by the current query part.
   */
  def findFirstRequiredOrder: Option[InterestingOrder] = {
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      Some(interestingOrder)
    } else {
      tail.flatMap { nextPart =>
        nextPart
          .findFirstRequiredOrder
          .map(reverseProjectedInterestingOrder(_, horizon, nextPart.queryGraph.argumentIds))
          .filter(_.requiredOrderCandidate.nonEmpty)
      }
    }
  }

  def isCoveredByHints(other: SinglePlannerQuery): Boolean = allHints.forall(other.allHints.contains)

  override def allHints: Set[Hint] = {
    val headHints = queryGraph.allHints ++ horizon.allHints
    tail.fold(headHints)(_.allHints ++ headHints)
  }

  override def visitHints[A](acc: A)(f: (A, Hint, QueryGraph) => A): A = {
    SinglePlannerQuery.visitHints(this, acc, f)
  }

  override def numHints: Int = allHints.size

  def amendQueryGraph(f: QueryGraph => QueryGraph): SinglePlannerQuery = withQueryGraph(f(queryGraph))

  def updateHorizon(f: QueryHorizon => QueryHorizon): SinglePlannerQuery = withHorizon(f(horizon))

  def updateQueryProjection(f: QueryProjection => QueryProjection): SinglePlannerQuery = horizon match {
    case projection: QueryProjection => withHorizon(f(projection))
    case _ => throw new InternalException("Tried updating projection when there was no projection there")
  }

  def updateTail(f: SinglePlannerQuery => SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None            => this
    case Some(tailQuery) => copy(tail = Some(f(tailQuery)))
  }

  def updateTailOrSelf(f: SinglePlannerQuery => SinglePlannerQuery): SinglePlannerQuery = tail match {
    case None    => f(this)
    case Some(_) => this.updateTail(_.updateTailOrSelf(f))
  }

  def tailOrSelf: SinglePlannerQuery = tail match {
    case None    => this
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
          queryInput = either(queryInput, other.queryInput)
        )

      case _ =>
        throw new InternalException("Tried to concatenate non-regular query projections")
    }
  }

  private def either[T](a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(aa), Some(bb)) => throw new InternalException(s"Can't join two query graphs. First: $aa, Second: $bb")
    case (s @ Some(_), None)  => s
    case (None, s)            => s
  }

  // This is here to stop usage of copy from the outside
  protected def copy(
    queryGraph: QueryGraph = queryGraph,
    interestingOrder: InterestingOrder = interestingOrder,
    horizon: QueryHorizon = horizon,
    tail: Option[SinglePlannerQuery] = tail,
    input: Option[Seq[Variable]] = queryInput
  ): SinglePlannerQuery

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
        case None         => nextAcc
      }
    }

    recurse(in, this)
  }

  override lazy val allQGsWithLeafInfo: collection.Seq[QgWithLeafInfo] =
    allPlannerQueries.flatMap(q => q.queryGraph.allQGsWithLeafInfo ++ q.horizon.allQueryGraphs)

  // Returns list of planner query and all of its tails
  def allPlannerQueries: collection.Seq[SinglePlannerQuery] = {
    val buffer = scala.collection.mutable.ArrayBuffer[SinglePlannerQuery]()
    var current = this
    while (current != null) {
      buffer += current
      current = current.tail.orNull
    }
    buffer
  }

  lazy val firstLabelInfo: Map[LogicalVariable, Set[LabelName]] =
    extractLabelInfo(this)

  lazy val lastLabelInfo: Map[LogicalVariable, Set[LabelName]] =
    extractLabelInfo(last)

  override def returns: Set[String] = {
    lastQueryHorizon match {
      case projection: QueryProjection => projection.keySet.map(_.name)
      case _                           => Set.empty
    }
  }

  override def asSinglePlannerQuery: SinglePlannerQuery = this

  override lazy val flattenForeach: SinglePlannerQuery = {
    val flatUpdates = queryGraph.mutatingPatterns.collect {
      case ForeachPattern(_, _, innerUpdates) =>
        innerUpdates.flattenForeach.fold(Seq.empty[MutatingPattern]) {
          case (updates, query) => updates ++ query.queryGraph.mutatingPatterns
        }
      case mutatingPattern => Seq(mutatingPattern)
    }.flatten

    val other = copy(
      queryGraph = queryGraph.withMutatingPattern(mutatingPatterns = flatUpdates),
      tail = tail.map(_.flattenForeach)
    ).updateHorizon {
      case csh: CallSubqueryHorizon => csh.copy(callSubquery = csh.callSubquery.flattenForeach)
      case horizon                  => horizon
    }

    // Optimization to keep lazy vals that have been computed already
    if (other == this) this else other
  }
}

object SinglePlannerQuery {
  def empty: RegularSinglePlannerQuery = RegularSinglePlannerQuery()

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]): Set[String] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds.map(_.name))
    patternNodeIds ++ patternRelIds
  }

  /**
   * Rename and filter the columns in an interesting order to before a given horizon.
   *
   * @param order       the InterestingOrder
   * @param horizon     the horizon
   * @param argumentIds the arguments to the next query part
   */
  def reverseProjectedInterestingOrder(
    order: InterestingOrder,
    horizon: QueryHorizon,
    argumentIds: Set[String]
  ): InterestingOrder = {
    horizon match {
      case qp: QueryProjection =>
        order.withReverseProjectedColumns(qp.projections, argumentIds.map(varFor))
      case _ => order.withReverseProjectedColumns(Map.empty, argumentIds.map(varFor))
    }
  }

  def extractLabelInfo(q: SinglePlannerQuery): Map[LogicalVariable, Set[LabelName]] = {
    val labelInfo = q.queryGraph.selections.labelInfo
    val projectedLabelInfo = q.horizon match {
      case projection: QueryProjection =>
        projection.projections.collect {
          case (projectedVar, v: Variable) if labelInfo.contains(v) =>
            projectedVar -> labelInfo(v)
        }
      case _ => Map.empty[LogicalVariable, Set[LabelName]]
    }
    labelInfo ++ projectedLabelInfo
  }

  private def visitHints[A](query: SinglePlannerQuery, acc: A, f: (A, Hint, QueryGraph) => A): A = {
    query.fold(acc) { case (acc, query) =>
      val qgAcc = query.queryGraph.hints.foldLeft(acc)(f(_, _, query.queryGraph))
      val optAcc = query.queryGraph.optionalMatches.foldLeft(qgAcc) {
        case (acc, optQg) => optQg.hints.foldLeft(acc)(f(_, _, optQg))
      }
      val horizonAcc = query.horizon match {
        case subqueryHorizon: CallSubqueryHorizon => subqueryHorizon.callSubquery.visitHints(optAcc)(f)
        case _                                    => optAcc
      }
      horizonAcc
    }
  }
}

case class RegularSinglePlannerQuery(
  queryGraph: QueryGraph = QueryGraph.empty,
  interestingOrder: InterestingOrder = InterestingOrder.empty,
  horizon: QueryHorizon = QueryProjection.empty,
  tail: Option[SinglePlannerQuery] = None,
  queryInput: Option[Seq[Variable]] = None
) extends SinglePlannerQuery {

  // This is here to stop usage of copy from the outside
  override protected def copy(
    queryGraph: QueryGraph = queryGraph,
    interestingOrder: InterestingOrder = interestingOrder,
    horizon: QueryHorizon = horizon,
    tail: Option[SinglePlannerQuery] = tail,
    queryInput: Option[Seq[Variable]] = queryInput
  ): SinglePlannerQuery =
    RegularSinglePlannerQuery(queryGraph, interestingOrder, horizon, tail, queryInput)

  override def dependencies: Set[String] =
    horizon.dependencies.map(_.name) ++ queryGraph.dependencies ++ tail.map(_.dependencies).getOrElse(Set.empty)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RegularSinglePlannerQuery]

  override def equals(other: Any): Boolean = other match {
    // Make sure it corresponds with pointOutDifference
    case that: RegularSinglePlannerQuery =>
      (that canEqual this) &&
      queryInput == that.queryInput &&
      queryGraph == that.queryGraph &&
      horizon == that.horizon &&
      tail == that.tail &&
      interestingOrder.requiredOrderCandidate.order == that.interestingOrder.requiredOrderCandidate.order
    case _ => false
  }

  private var theHashCode: Int = -1

  override def hashCode(): Int = {
    if (theHashCode == -1) {
      val state: Seq[Object] = Seq(queryInput, queryGraph, horizon, tail, interestingOrder.requiredOrderCandidate.order)
      theHashCode = MurmurHash3.seqHash(state)
    }
    theHashCode
  }

  /**
   * Make sure that tags are of the same length, so that the differences nicely align.
   * @param tag tag to print
   * @param length desired length of the output
   */
  def formatTag(tag: String, length: Int): String =
    s"$tag:".padTo(length, ' ')

  def pointOutDifference(other: RegularSinglePlannerQuery, thisTag: String, otherTag: String): String = {
    val tagLength = Math.max(thisTag.length, otherTag.length) + 1

    // Make sure it corresponds with equals
    val builder = new mutable.StringBuilder()
    builder.append("Differences:\n")
    if (queryInput != other.queryInput) {
      builder.append(" - QueryInput\n")
      builder.append(s"    ${formatTag(thisTag, tagLength)} $queryInput\n")
      builder.append(s"    ${formatTag(otherTag, tagLength)} ${other.queryInput}\n")
    }
    if (queryGraph != other.queryGraph) {
      builder.append(" - QueryGraph\n")
      builder.append(s"    ${formatTag(thisTag, tagLength)} $queryGraph\n")
      builder.append(s"    ${formatTag(otherTag, tagLength)} ${other.queryGraph}\n")

    }
    if (horizon != other.horizon) {
      builder.append(" - Horizon\n")
      builder.append(s"    ${formatTag(thisTag, tagLength)} $horizon\n")
      builder.append(s"    ${formatTag(otherTag, tagLength)} ${other.horizon}\n")
    }
    if (tail != other.tail) {
      builder.append(" - Tail\n")
      builder.append(s"    ${formatTag(thisTag, tagLength)} $tail\n")
      builder.append(s"    ${formatTag(otherTag, tagLength)} ${other.tail}\n")
    }
    if (interestingOrder.requiredOrderCandidate.order != other.interestingOrder.requiredOrderCandidate.order) {
      builder.append(" - interestingOrder.requiredOrderCandidate.order\n")
      builder.append(s"    ${formatTag(thisTag, tagLength)} ${interestingOrder.requiredOrderCandidate.order}\n")
      builder.append(s"    ${formatTag(otherTag, tagLength)} ${other.interestingOrder.requiredOrderCandidate.order}\n")
    }
    builder.toString()
  }
}
