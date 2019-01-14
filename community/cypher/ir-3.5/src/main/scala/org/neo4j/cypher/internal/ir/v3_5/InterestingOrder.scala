/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.v3_5.expressions.{Expression, Property, PropertyKeyName, Variable}

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

  val empty = InterestingOrder(Seq.empty, Seq.empty)

  def asc(id: String): InterestingOrder = empty.asc(id)
  def desc(id: String): InterestingOrder = empty.desc(id)
  def ascInteresting(id: String): InterestingOrder = empty.ascInteresting(id)
  def descInteresting(id: String): InterestingOrder = empty.descInteresting(id)
}
/**
  * A single PlannerQuery can require an ordering on its results. This ordering can emerge
  * from an ORDER BY, or even from distinct or aggregation. The requirement on the ordering can
  * be strict (it needs to be ordered for correct results) or weak (ordering will allow for optimizations).
  * A weak requirement might in addition not care about column order and sort direction.
  *
  * Right now this type only encodes strict requirements that emerged because of an ORDER BY.
  *
  * @param required    a sequence of required sort directions of columns
  * @param interesting a sequence of interesting sort directions of columns
  */
case class InterestingOrder(required: Seq[InterestingOrder.ColumnOrder],
                            interesting: Seq[InterestingOrder.ColumnOrder] = Seq.empty) {

  import InterestingOrder._

  val isEmpty: Boolean = required.isEmpty && interesting.isEmpty

  def asc(id: String): InterestingOrder = InterestingOrder(required :+ Asc(id), interesting)
  def desc(id: String): InterestingOrder = InterestingOrder(required :+ Desc(id), interesting)
  def ascInteresting(id: String): InterestingOrder = InterestingOrder(required, interesting :+ Asc(id))
  def descInteresting(id: String): InterestingOrder = InterestingOrder(required, interesting :+ Desc(id))

  def asInteresting: InterestingOrder = InterestingOrder(Seq.empty, required ++ interesting)

  def headOption: Option[InterestingOrder.ColumnOrder] =
    required.headOption.orElse(interesting.headOption)

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

    InterestingOrder(project(required), project(interesting))
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

    InterestingOrder(rename(required), rename(interesting))
  }

  /**
    * Checks if a RequiredOrder is satisfied by a ProvidedOrder
    */
  def satisfiedBy(providedOrder: ProvidedOrder): Boolean = {
    required.zipAll(providedOrder.columns, null, null).forall {
      case (null, _) => true // no required order left
      case (_, null) => false // required order left but no provided
      case (InterestingOrder.Asc(requiredId), ProvidedOrder.Asc(providedId)) => requiredId == providedId
      case (InterestingOrder.Desc(requiredId), ProvidedOrder.Desc(providedId)) => requiredId == providedId
      case _ => false
    }
  }
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
