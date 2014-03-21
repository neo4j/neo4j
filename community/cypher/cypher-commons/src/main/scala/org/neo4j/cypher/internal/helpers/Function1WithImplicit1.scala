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
package org.neo4j.cypher.internal.helpers

trait Function0WithImplicit1[R, I1] {
  def apply()(implicit i1: I1): R

  def andThen[A](g: Function1WithImplicit1[R, A, I1]): Function0WithImplicit1[A, I1] = {
    val that = this
    new Function0WithImplicit1[A, I1] {
      def apply()(implicit i1: I1): A = g(that.apply())
    }
  }
}

trait Function1WithImplicit1[T1, R, I1] {
  def apply(v1: T1)(implicit i1: I1): R

  def andThen[A](g: Function1WithImplicit1[R, A, I1]): Function1WithImplicit1[T1, A, I1] = {
    val that = this
    new Function1WithImplicit1[T1, A, I1] {
      def apply(x: T1)(implicit i1: I1): A = g(that.apply(x))
    }
  }

  def compose[A](g: Function1WithImplicit1[A, T1, I1]): Function1WithImplicit1[A, R, I1] = {
    val that = this
    new Function1WithImplicit1[A, R, I1] {
      def apply(x: A)(implicit i1: I1): R = that.apply(g(x))
    }
  }
}
