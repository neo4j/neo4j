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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

sealed trait ZeroOneOrMany[+T]

object ZeroOneOrMany {
  def apply[T](elts: Seq[T]) = elts match {
    case Seq() => Zero
    case Seq(one) => One(one)
    case many => Many(many)
  }
}

case object Zero extends ZeroOneOrMany[Nothing]
final case class One[T](value: T) extends ZeroOneOrMany[T]
final case class Many[T](values: Seq[T]) extends ZeroOneOrMany[T]
