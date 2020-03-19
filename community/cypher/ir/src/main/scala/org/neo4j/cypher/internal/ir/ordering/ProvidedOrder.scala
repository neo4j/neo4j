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
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder.OrderOrigin
import org.neo4j.cypher.internal.util.NonEmptyList

object ProvidedOrder {

  object Column {
    def unapply(arg: Column): Option[Expression] = {
      Some(arg.expression)
    }
    def apply(expression: Expression, ascending: Boolean): Column = {
      if (ascending) Asc(expression) else Desc(expression)
    }
  }

  sealed trait Column {
    def expression: Expression
    def isAscending: Boolean
  }

  case class Asc(expression: Expression) extends Column {
    override val isAscending: Boolean = true
  }
  case class Desc(expression: Expression) extends Column {
    override val isAscending: Boolean = false
  }

  // ---

  /**
   * The origin of a provided order.
   * If a plan introduced the ordering itself: [[Self]].
   * If it kept or modified a provided order from the left or right, [[Left]] or [[Right]].
   */
  sealed trait OrderOrigin
  case object Self extends OrderOrigin
  case object Left  extends OrderOrigin
  case object Right extends OrderOrigin

  // ---

  def apply(columns: Seq[ProvidedOrder.Column], orderOrigin: OrderOrigin): ProvidedOrder = {
    if (columns.isEmpty) NoProvidedOrder
    else NonEmptyProvidedOrder(NonEmptyList.from(columns), orderOrigin)
  }

  def unapply(arg: ProvidedOrder): Option[Seq[ProvidedOrder.Column]] = arg match {
    case NoProvidedOrder => Some(Seq.empty)
    case NonEmptyProvidedOrder(allColumns, _) => Some(allColumns.toIndexedSeq)
  }

  val empty: ProvidedOrder = NoProvidedOrder

  def asc(expression: Expression): NonEmptyProvidedOrder = NonEmptyProvidedOrder(NonEmptyList(Asc(expression)), Self)
  def desc(expression: Expression): NonEmptyProvidedOrder = NonEmptyProvidedOrder(NonEmptyList(Desc(expression)), Self)
}

/**
 * A LogicalPlan can guarantee to provide its results in a particular order. This trait
 * is used for the purpose of conveying the information of which order the results are in,
 * if they are in any defined order.
 */
sealed trait ProvidedOrder {
  /**
   * @return sequence of columns with sort direction
   */
  def columns: Seq[ProvidedOrder.Column]

  /**
   * @return whether this ProvidedOrder is empty
   */
  def isEmpty: Boolean

  /**
   * @return the origin of the order, or None, if this is empty.
   */
  def orderOrigin: Option[OrderOrigin]

  /**
   * Returns a new provided order where the order columns of this are concatenated with
   * the order columns of the other provided order. Example:
   * [n.foo ASC, n.bar DESC].followedBy([n.baz ASC]) = [n.foo ASC, n.bar DESC, n.baz ASC]
   *
   * If this is empty, then the returned provided order will also be empty, regardless of the
   * given nextOrder.
   */
  def followedBy(nextOrder: ProvidedOrder): ProvidedOrder

  /**
   * Trim provided order up until a sort column that matches any of the given args.
   */
  def upToExcluding(args: Set[String]): ProvidedOrder

  /**
   * Map the columns with some mapping function
   */
  def mapColumns(f: ProvidedOrder.Column => ProvidedOrder.Column): ProvidedOrder

  /**
   * The same order columns, but with OrderOrigin = [[Left]]
   */
  def fromLeft: ProvidedOrder

  /**
   * The same order columns, but with OrderOrigin = [[Right]]
   */
  def fromRight: ProvidedOrder
}

case object NoProvidedOrder extends ProvidedOrder {
  override def columns: Seq[ProvidedOrder.Column] = Seq.empty
  override def isEmpty: Boolean = true
  override def orderOrigin: Option[OrderOrigin] = None
  override def followedBy(nextOrder: ProvidedOrder): ProvidedOrder = this
  override def upToExcluding(args: Set[String]): ProvidedOrder = this
  override def mapColumns(f: ProvidedOrder.Column => ProvidedOrder.Column): ProvidedOrder = this
  override def fromLeft: ProvidedOrder = this
  override def fromRight: ProvidedOrder = this
}

case class NonEmptyProvidedOrder(allColumns: NonEmptyList[ProvidedOrder.Column], theOrderOrigin: OrderOrigin) extends ProvidedOrder {

  override def columns: Seq[ProvidedOrder.Column] = allColumns.toIndexedSeq

  override def isEmpty: Boolean = false

  override def orderOrigin: Option[OrderOrigin] = Some(theOrderOrigin)

  def asc(expression: Expression): NonEmptyProvidedOrder = NonEmptyProvidedOrder(allColumns :+ Asc(expression), theOrderOrigin)
  def desc(expression: Expression): NonEmptyProvidedOrder = NonEmptyProvidedOrder(allColumns :+ Desc(expression), theOrderOrigin)

  override def fromLeft: NonEmptyProvidedOrder = copy(theOrderOrigin = ProvidedOrder.Left)
  override def fromRight: NonEmptyProvidedOrder = copy(theOrderOrigin = ProvidedOrder.Right)

  override def mapColumns(f: ProvidedOrder.Column => ProvidedOrder.Column): NonEmptyProvidedOrder = copy(allColumns = allColumns.map(f))

  override def followedBy(nextOrder: ProvidedOrder): NonEmptyProvidedOrder = {
    NonEmptyProvidedOrder(allColumns :++ nextOrder.columns, theOrderOrigin)
  }

  override def upToExcluding(args: Set[String]): ProvidedOrder = {
    val (_, trimmed) = columns.foldLeft((false,Seq.empty[ProvidedOrder.Column])) {
      case (acc, _) if acc._1 => acc
      case (acc, col) if args.contains(col.expression.asCanonicalStringVal) => (true, acc._2)
      case (acc, col) => (acc._1, acc._2 :+ col)
    }
    if (trimmed.isEmpty) {
      NoProvidedOrder
    } else {
      NonEmptyProvidedOrder(NonEmptyList.from(trimmed), theOrderOrigin)
    }
  }
}
