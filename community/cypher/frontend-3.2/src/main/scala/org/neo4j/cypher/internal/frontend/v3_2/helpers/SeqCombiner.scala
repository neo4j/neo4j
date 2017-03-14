/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.helpers

object SeqCombiner {
  /**
    * Combines each element in the inner Seq's with one element of every other inner Seq.
    *
    * scala> combine(List(List("A", "B", "C"), List(1,2,3), List("x", "y", "z")))
      res0: Seq[Seq[Any]] = List(
          List(A, 1, x),
          List(A, 1, y),
          List(A, 1, z),
          List(A, 2, x),
          List(A, 2, y),
          List(A, 2, z),
          List(A, 3, x),
          List(A, 3, y),
          List(A, 3, z),
          List(B, 1, x),
          List(B, 1, y),
          List(B, 1, z),
          List(B, 2, x),
          List(B, 2, y),
          List(B, 2, z),
          List(B, 3, x),
          List(B, 3, y),
          List(B, 3, z),
          List(C, 1, x),
          List(C, 1, y),
          List(C, 1, z),
          List(C, 2, x),
          List(C, 2, y),
          List(C, 2, z),
          List(C, 3, x),
          List(C, 3, y),
          List(C, 3, z))
    */
  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x; b <- y) yield a :+ b
    }

}
