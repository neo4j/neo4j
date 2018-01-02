/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders


abstract sealed class QueryToken[T](val token: T) {
  def solved: Boolean

  def unsolved = !solved

  def solve: QueryToken[T] = Solved(token)

  def map[B](f : T => B):QueryToken[B] = if (solved) Solved(f(token)) else Unsolved(f(token))
}

case class Solved[T](t: T) extends QueryToken[T](t) {
  val solved = true
}

case class Unsolved[T](t: T) extends QueryToken[T](t) {
  val solved = false
}


object QueryToken {
  def unapply(v: Any): Option[Any] = v match {
    case q: QueryToken[_] => Some(q.token)
    case _                => None
  }
}
