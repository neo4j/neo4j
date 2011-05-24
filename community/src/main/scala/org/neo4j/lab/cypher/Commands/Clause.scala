/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 13:29 
 */


abstract sealed class Clause {
  def ++(other: Clause): Clause = And(this, other)
  def hasOrs : Boolean = false
}

case class StringEquals(variable: String, propName: String, value: String) extends Clause

case class NumberLargerThan(variable: String, propName: String, value: Float) extends Clause

case class And(a: Clause, b: Clause) extends Clause {
  override def hasOrs = a.hasOrs || b.hasOrs
}

case class Or(a: Clause, b: Clause) extends Clause {
  override def hasOrs = true
}
