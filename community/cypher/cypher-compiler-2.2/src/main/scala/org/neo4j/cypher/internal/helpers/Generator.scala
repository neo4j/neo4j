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

trait Generator[+T] extends Iterable[T] {
  self =>

  def fetchNext: DeliveryState
  def deliverNext: T

  def iterator = new GeneratorIterator[T](self)
}

sealed abstract class GeneratorState {
  def fetch(generator: Generator[Any]): DeliveryState
}

case object ReadyToFetch extends GeneratorState {
  def fetch(generator: Generator[Any]): DeliveryState = generator.fetchNext
}

sealed abstract class DeliveryState extends GeneratorState {
  self =>

  def fetch(generator: Generator[Any]): DeliveryState = self

  def canDeliver: Boolean
  def deliver[T](generator: Generator[T]): T
}

case object ReadyToDeliver extends DeliveryState {
  def canDeliver: Boolean = true
  def deliver[T](generator: Generator[T]): T = generator.deliverNext
}

case object NothingToDeliver extends DeliveryState {
  def canDeliver: Boolean = false
  def deliver[T](generator: Generator[T]): T = Iterator.empty.next()
}

class GeneratorIterator[+T](private val generator: Generator[T]) extends Iterator[T] {
  private var state: GeneratorState = ReadyToFetch

  def hasNext: Boolean = {
    val deliveryState = state.fetch(generator)
    state = deliveryState
    deliveryState.canDeliver
  }

  def next(): T = {
    val deliveryState = state.fetch(generator)
    val result = deliveryState.deliver(generator)
    state = ReadyToFetch
    result
  }

  def close() = {
    state = NothingToDeliver
  }

  override def toString(): String =
    s"${getClass.getName}@${Integer.toHexString(hashCode())}(state = $state)"
}

