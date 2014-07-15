/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

case class Effects(value: Int)  {
  def &(other: Effects): Effects = Effects(other.value & value)
  def |(other: Effects): Effects = Effects(other.value | value)

  def contains(other: Effects): Boolean = (value & other.value) == other.value
  def intersects(other: Effects): Boolean = (value & other.value) != 0
  def reads(): Boolean = intersects(Effects.READS)
  def writes(): Boolean = intersects(Effects.WRITES)

  override def toString =
    if (value == 0) "NONE"
    else {
      Seq("WRITES_NODES", "WRITES_RELATIONSHIPS", "READS_NODES", "READS_RELATIONSHIPS").zipWithIndex.filter { case (_: String, index: Int) => (value & (2 << index)) != 0}.map(_._1).mkString(" | ")
    }
}

object Effects {
  val WRITES_NODES = Effects(2 << 0)
  val WRITES_RELATIONSHIPS = Effects(2 << 1)
  val READS_NODES = Effects(2 << 2)
  val READS_RELATIONSHIPS = Effects(2 << 3)

  val NONE = Effects(0)
  val READS = READS_NODES | READS_RELATIONSHIPS
  val WRITES = WRITES_NODES | WRITES_RELATIONSHIPS
  val ALL = READS | WRITES
}
