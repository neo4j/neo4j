/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

class NiceHasher(val original: Seq[Any]) {
  override def equals(p1: Any): Boolean = {
    if(p1 == null || !p1.isInstanceOf[NiceHasher])
      return false
    
    val other = p1.asInstanceOf[NiceHasher]

    comperableValues.equals(other.comperableValues)
  }
  
  def comperableValues = original.map {
    case x:Array[_] => x.deep
    case x => x
  }

  override def toString = hashCode() + " : " + original.toString

  private lazy val hash = original.map {
    case x: Array[AnyRef] => java.util.Arrays.deepHashCode(x)
    case null => 0
    case x => x.hashCode()
  }.foldLeft(0)((a, b) => 31 * a + b)

  override def hashCode() = hash
}