/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.ir.v4_0

import org.neo4j.cypher.internal.ir.v4_0.InterestingOrder.{Asc, ColumnOrder, Desc}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property, PropertyKeyName, Variable}

object InterestingOrder {
  sealed trait ColumnOrder {
    def id: String
    def withId(newId: String): ColumnOrder
  }
  case class Asc(id: String) extends ColumnOrder {
    override def withId(newId: String): ColumnOrder = Asc(newId)
  }
  case class Desc(id: String) extends ColumnOrder {
    override def withId(newId: String): ColumnOrder = Desc(newId)
  }

  val empty = InterestingOrder(RequiredOrderCandidate.empty, Seq.empty)

  def required(candidate: RequiredOrderCandidate): InterestingOrder = InterestingOrder(candidate, Seq.empty)
  def interested(candidate: InterestingOrderCandidate): InterestingOrder = InterestingOrder(RequiredOrderCandidate.empty, Seq(candidate))
}
/**
  * A single PlannerQuery can require an ordering on its results. This ordering can emerge
  * from an ORDER BY, or even from distinct or aggregation. The requirement on the ordering can
  * be strict (it needs to be ordered for correct results) or weak (ordering will allow for optimizations).
  * A weak requirement might in addition not care about column order and sort direction.
  *
  * Right now this type only encodes strict requirements that emerged because of an ORDER BY.
  *
  * @param requiredOrderCandidate    a candidate for required sort directions of columns
  * @param interestingOrderCandidates a sequence of candidates for interesting sort directions of columns
  */
case class InterestingOrder(requiredOrderCandidate: RequiredOrderCandidate,
                            interestingOrderCandidates: Seq[InterestingOrderCandidate] = Seq.empty) {

  import InterestingOrder._

  val isEmpty: Boolean = requiredOrderCandidate.isEmpty && interestingOrderCandidates.forall(_.isEmpty)

  // TODO maybe merge some candidates
  def interested(candidate: InterestingOrderCandidate): InterestingOrder = InterestingOrder(requiredOrderCandidate, interestingOrderCandidates :+ candidate)

  // TODO maybe merge some candidates
  def asInteresting: InterestingOrder =
    if (requiredOrderCandidate.isEmpty) this
    else InterestingOrder(RequiredOrderCandidate.empty,
      interestingOrderCandidates :+ requiredOrderCandidate.asInteresting)

  def withProjectedColumns(projectExpressions: Map[String, Expression]) : InterestingOrder = {
    def project(columns: Seq[ColumnOrder]): Seq[ColumnOrder] = {
      columns.map {
        case column@StringPropertyLookup(varName, propName) =>
          projectExpressions.collectFirst {
            case (newId, Property(Variable(`varName`), PropertyKeyName(`propName`))) => column.withId(newId)
            case (newId, Variable(`varName`)) => column.withId(newId + "." + propName)
          }.getOrElse(column)

        case column: ColumnOrder =>
          val varName = column.id
          projectExpressions.collectFirst {
            case (newId, Variable(`varName`)) => column.withId(newId)
          }.getOrElse(column)
      }
    }

    InterestingOrder(requiredOrderCandidate.renameColumns(project),
      interestingOrderCandidates.map(_.renameColumns(project)).filter(!_.isEmpty))
  }

  def withReverseProjectedColumns(projectExpressions: Map[String, Expression], argumentIds: Set[String]) : InterestingOrder = {
    def columnIfArgument(column: ColumnOrder): Option[ColumnOrder] =
      if (argumentIds.contains(column.id)) Some(column) else None

    def rename(columns: Seq[ColumnOrder]): Seq[ColumnOrder] = {
      columns.flatMap {
        case column@StringPropertyLookup(varName, propName) =>
          projectExpressions.collectFirst {
            case (`varName`, Variable(newVarName)) => column.withId(newVarName + "." + propName)
          }.orElse(columnIfArgument(column))

        case column: ColumnOrder =>
          val varName = column.id
          projectExpressions.collectFirst {
            case (`varName`, Property(Variable(prevVarName), PropertyKeyName(prevPropName))) => column.withId(prevVarName + "." + prevPropName)
            case (`varName`, Variable(prevVarName)) => column.withId(prevVarName)
          }.orElse(columnIfArgument(column))
      }
    }

    InterestingOrder(requiredOrderCandidate.renameColumns(rename),
      interestingOrderCandidates.map(_.renameColumns(rename)).filter(!_.isEmpty))
  }

  /**
    * Checks if a RequiredOrder is satisfied by a ProvidedOrder
    */
  def satisfiedBy(providedOrder: ProvidedOrder): Boolean = {
    requiredOrderCandidate.order.zipAll(providedOrder.columns, null, null).forall {
      case (null, _) => true // no required order left
      case (_, null) => false // required order left but no provided
      case (InterestingOrder.Asc(requiredId), ProvidedOrder.Asc(providedId)) => requiredId == providedId
      case (InterestingOrder.Desc(requiredId), ProvidedOrder.Desc(providedId)) => requiredId == providedId
      case _ => false
    }
  }

  /**
    * //Should be removed next commit (fixing review coments) so HEAD doesn't matter
    */
  def interestingSatisfiedBy(providedOrder: ProvidedOrder): Boolean = {
    interestingOrderCandidates.head.order.zipAll(providedOrder.columns, null, null).forall {
      case (null, _) => true // no interesting order left
      case (_, null) => false // interesting order left but no provided
      case (InterestingOrder.Asc(interestingId), ProvidedOrder.Asc(providedId)) => interestingId == providedId
      case (InterestingOrder.Desc(interestingId), ProvidedOrder.Desc(providedId)) => interestingId == providedId
      case _ => false
    }
  }
}

// TODO put this somewhere else
trait OrderCandidate {
  def order: Seq[ColumnOrder]

  def isEmpty: Boolean = order.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def headOption: Option[ColumnOrder] = order.headOption

  def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): OrderCandidate

  def asc(id: String): OrderCandidate

  def desc(id: String): OrderCandidate
}

case class RequiredOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate {
  def asInteresting: InterestingOrderCandidate = InterestingOrderCandidate(order)

  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): RequiredOrderCandidate = RequiredOrderCandidate(f(order))

  override def asc(id: String): RequiredOrderCandidate = RequiredOrderCandidate(order :+ Asc(id))

  override def desc(id: String): RequiredOrderCandidate = RequiredOrderCandidate(order :+ Desc(id))
}

object RequiredOrderCandidate {
  def empty: RequiredOrderCandidate = RequiredOrderCandidate(Seq.empty)

  def asc(id: String): RequiredOrderCandidate = empty.asc(id)
  def desc(id: String): RequiredOrderCandidate = empty.desc(id)
}

case class InterestingOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate {
  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): InterestingOrderCandidate = InterestingOrderCandidate(f(order))

  override def asc(id: String): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Asc(id))

  override def desc(id: String): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Desc(id))
}

object InterestingOrderCandidate {
  def empty: InterestingOrderCandidate = InterestingOrderCandidate(Seq.empty)

  def asc(id: String): InterestingOrderCandidate = empty.asc(id)

  def desc(id: String): InterestingOrderCandidate = empty.desc(id)
}

/**
  * Split the id into varName and propName, if the
  * ordered column is a property lookup.
  */
object StringPropertyLookup {

  def unapply(arg: InterestingOrder.ColumnOrder): Option[(String, String)] = {
    arg.id.split("\\.", 2) match {
      case Array(varName, propName) => Some((varName, propName))
      case _ => None
    }
  }

  def unapply(arg: String): Option[(String, String)] = {
    arg.split("\\.", 2) match {
      case Array(varName, propName) => Some((varName, propName))
      case _ => None
    }
  }
}
