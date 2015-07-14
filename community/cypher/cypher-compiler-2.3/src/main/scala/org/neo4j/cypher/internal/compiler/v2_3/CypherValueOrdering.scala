/*
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
package org.neo4j.cypher.internal.compiler.v2_3

object CypherValueOrdering extends Ordering[Any] {

  override def compare(x: Any, y: Any) = (x, y) match {
    case (null, null) => 0
    case (null, _) => +1
    case (_, null) => -1
    case (l: Number, r: Number) => CypherNumberOrdering.compare(l, r)
    case (l: String, r: String) => l.compareTo(r)
    case (l: Character, r: String) => l.toString.compareTo(r)
    case (l: String, r: Character) => l.compareTo(r.toString)
    case (l: Character, r: Character) => Character.compare(l, r)
    case _ => throw new IllegalArgumentException(s"Cannot compare '$x' with '$y'. They are incomparable.")
  }
}
