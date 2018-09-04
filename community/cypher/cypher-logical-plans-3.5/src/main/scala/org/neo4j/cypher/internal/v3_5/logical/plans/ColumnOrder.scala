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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.ir.v3_5.StringPropertyLookup

sealed trait ColumnOrder {
  def id: String

  def isAscending: Boolean
}

case class Ascending(id: String) extends ColumnOrder {
  override def isAscending: Boolean = true
}

case class Descending(id: String) extends ColumnOrder {
  override def isAscending: Boolean = false
}

object ColumnOrder {
  def apply(id: String, ascending: Boolean): ColumnOrder = {
    if (ascending) Ascending(id) else Descending(id)
  }

  def unapply(arg: ColumnOrder): Option[String] = {
    Some(arg.id)
  }
}

object ColumnOrderOfProperty {
  /**
    * Split the id into varName and propName, if the
    * ordered column is a property lookup.
    */
  def unapply(arg: ColumnOrder): Option[(String, String)] = {
    StringPropertyLookup.unapply(arg.id)
  }
}
