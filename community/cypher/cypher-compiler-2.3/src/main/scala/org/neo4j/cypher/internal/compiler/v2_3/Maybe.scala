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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.InternalException

abstract sealed class Maybe[+T] {
  def values: Seq[T]
  def success: Boolean
  def ++[B >: T](other: Maybe[B]): Maybe[B]
  def map[B](f: T => B): Maybe[B]
  def seqMap[B](f:Seq[T]=>Seq[B]): Maybe[B]
  def getValuesOr[B >: T](f: => Seq[B]): Seq[B]
}

case class Yes[T](values: Seq[T]) extends Maybe[T] {
  def success = true

  def ++[B >: T](other: Maybe[B]): Maybe[B] = other match {
    case Yes(otherStuff) => Yes(values ++ otherStuff)
    case No(msg) => No(msg)
  }

  def map[B](f: T => B): Maybe[B] = Yes(values.map(f))

  def seqMap[B](f: (Seq[T]) => Seq[B]): Maybe[B] = Yes(f(values))

  def getValuesOr[B >: T](f: => Seq[B]) = values
}

case class No(messages: Seq[String]) extends Maybe[Nothing] {
  def values = throw new InternalException("No values exists")
  def success = false

  def ++[B >: Nothing](other: Maybe[B]): Maybe[B] = other match {
    case Yes(_) => this
    case No(otherMessages) => No(messages ++ otherMessages)
  }

  def map[B](f: Nothing => B): Maybe[B] = this

  def seqMap[B](f: (Seq[Nothing]) => Seq[B]): Maybe[B] = this

  def getValuesOr[B >: Nothing](f: => Seq[B]) = f
}
