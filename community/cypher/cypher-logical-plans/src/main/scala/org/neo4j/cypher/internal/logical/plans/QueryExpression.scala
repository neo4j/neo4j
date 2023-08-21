/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

sealed trait QueryExpression[+T] {

  def expressions: Seq[T]

  def map[R](f: T => R): QueryExpression[R]
}

sealed trait SingleExpression[+T] {

  def expression: T

  def expressions: Seq[T] = Seq(expression)
}

case class SingleQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = SingleQueryExpression(f(expression))
}

case class ManyQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = ManyQueryExpression(f(expression))
}

case class RangeQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = RangeQueryExpression(f(expression))
}

case class CompositeQueryExpression[T](inner: Seq[QueryExpression[T]]) extends QueryExpression[T] {
  def map[R](f: T => R) = CompositeQueryExpression(inner.map(_.map(f)))

  def expressions: Seq[T] = inner.flatMap(_.expressions)

  def exactOnly: Boolean =
    inner.forall(p => p.isInstanceOf[SingleQueryExpression[T]] || p.isInstanceOf[ManyQueryExpression[T]])
}

case class ExistenceQueryExpression[T]() extends QueryExpression[T] {
  def map[R](f: T => R) = ExistenceQueryExpression()

  def expressions: Seq[T] = Seq.empty
}
