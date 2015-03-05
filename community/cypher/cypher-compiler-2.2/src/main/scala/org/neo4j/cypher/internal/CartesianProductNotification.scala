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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v2_2.InputPosition
import org.neo4j.graphdb.notification
import org.neo4j.graphdb.notification.{NotificationCode, NotificationSeverityLevel, Notification}

import scala.beans.BeanProperty

/**
 * Notification for unwanted cartesian products found in a query.
 *
 * This class is intended for use both from java and scala, hence
 * the duplication of scala style and java style getters.
 *
 * @param position the position of the unwanted cartesian product
 */
case class CartesianProductNotification(position: InputPosition) extends Notification {

  def this(offset:Int, line: Int, column: Int) = this(InputPosition(offset, line, column))

  @BeanProperty
  val code = NotificationCode.CARTESIAN_PRODUCT

  @BeanProperty
  val severity = NotificationSeverityLevel.WARN

  @BeanProperty
  val description = "If a part of a query contains multiple disconnected patterns, this will build a " +
    "cartesian product between all those parts. This may produce a large amount of data and slow down query processing. " +
    "While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross " +
    "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH"

  @BeanProperty
  val  title = "This query builds a cartesian product between disconnected patterns."

  //returns the corresponding java InputPosition
  val getPosition = new notification.InputPosition(position.offset, position.line, position.column)
}
