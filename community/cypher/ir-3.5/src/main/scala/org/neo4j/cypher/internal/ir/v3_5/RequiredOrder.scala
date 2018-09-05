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
package org.neo4j.cypher.internal.ir.v3_5

import org.opencypher.v9_0.expressions.{Expression, Property, PropertyKeyName, Variable}

sealed trait RequiredColumnOrder
case object AscColumnOrder extends RequiredColumnOrder
case object DescColumnOrder extends RequiredColumnOrder

object RequiredOrder {
  val empty = RequiredOrder(Seq.empty)
}

/**
  * A single PlannerQuery can require an ordering on its results. This ordering can emerge
  * from an ORDER BY, or even from distinct or aggregation. The requirement on the ordering can
  * be strict (it needs to be ordered for correct results) or weak (ordering will allow for optimizations).
  * A weak requirement might in addition not care about column order and sort direction.
  *
  * Right now this type only encodes strict requirements that emerged because of an ORDER BY.
  *
  * @param columns a sequence of columns names and their required sort direction.
  */
case class RequiredOrder(columns: Seq[(String, RequiredColumnOrder)]) {

  val isEmpty: Boolean = columns.isEmpty

  def withRenamedColumns(projectExpressions: Map[String, Expression]) : RequiredOrder = {
    val renamedColumns = columns.map {
      case column@(StringPropertyLookup(varName, propName), order) =>
        projectExpressions.collectFirst {
          case (newName, Property(Variable(`varName`), PropertyKeyName(`propName`))) => (newName, order)
          case (newName, Variable(`varName`)) => (newName + "." + propName, order)
        }.getOrElse(column)

      case column@(varName, order) =>
        projectExpressions.collectFirst {
          case (newName, Variable(`varName`)) => (newName, order)
        }.getOrElse(column)
    }
    RequiredOrder(renamedColumns)
  }

  /**
    * Checks if a RequiredOrder is satisfied by a ProvidedOrder
    */
  def satisfiedBy(orderedBy: ProvidedOrder): Boolean = {
    columns.zipAll(orderedBy.columns, null, null).forall {
      case (null, _) => true
      case (_, null) => false
      case ((name, AscColumnOrder), ProvidedOrder.Asc(id)) => name == id
      case ((name, DescColumnOrder), ProvidedOrder.Desc(id)) => name == id
      case _ => false
    }
  }
}

object StringPropertyLookup {
  /**
    * Split the id into varName and propName, if the
    * ordered column is a property lookup.
    */
  def unapply(arg: String): Option[(String, String)] = {
    arg.split("\\.", 2) match {
      case Array(varName, propName) => Some((varName, propName))
      case _ => None
    }
  }
}