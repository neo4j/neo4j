/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.predicates

import scala.collection.mutable

/**
  * This is a class that handles IN checking. With a cache. It's a state machine, and
  * each checking using the contains() method returns both the result of the IN check and the new state.
  */
trait Checker {
  def contains(value: Any): (Option[Boolean], Checker)
}

class BuildUp(iterator: Iterator[Any]) extends Checker {
  private val cachedSet: mutable.Set[Equivalent] = new mutable.HashSet[Equivalent]

  // If we don't return true, this is what we will return. If the collection contains any nulls, we'll return None,
  // else we return Some(false).
  private var falseResult: Option[Boolean] = Some(false)

  assert(iterator.nonEmpty)

  override def contains(value: Any): (Option[Boolean], Checker) = {
    if (value == null) (None, this)
    else {
      val eqValue = Equivalent(value)
      if (cachedSet.contains(eqValue))
        (Some(true), this)
      else
        checkAndBuildUpCache(value)
    }
  }

  private def checkAndBuildUpCache(value: Any): (Option[Boolean], Checker) = {
    var foundMatch = false
    while (iterator.nonEmpty && !foundMatch) {
      val nextValue = iterator.next()

      if (nextValue == null) {
        falseResult = None
      } else {
        val next = Equivalent(nextValue)
        cachedSet.add(next)
        foundMatch = next == value
      }
    }

    if (cachedSet.isEmpty) {
      (None, NullListChecker)
    } else {
      val nextState = if (iterator.nonEmpty) this else new SetChecker(cachedSet, falseResult)
      val result = if (foundMatch) Some(true) else falseResult

      (result, nextState)
    }
  }
}

case object AlwaysFalseChecker extends Checker {
  override def contains(value: Any): (Option[Boolean], Checker) = (Some(false), this)
}

case object NullListChecker extends Checker {
  override def contains(value: Any): (Option[Boolean], Checker) = (None, this)
}

// This is the final form for this cache.
class SetChecker(cachedSet: mutable.Set[Equivalent], falseResult: Option[Boolean]) extends Checker {

  assert(cachedSet.nonEmpty)

  override def contains(value: Any): (Option[Boolean], Checker) = {
    if (value == null)
      (None, this)
    else {
      val eqValue = Equivalent(value)

      val exists = cachedSet.contains(eqValue)
      val result = if (exists) Some(true) else falseResult
      (result, this)
    }
  }
}
