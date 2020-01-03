/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import java.util

import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.{AnyValue, Equality}

import scala.collection.mutable

/**
  * This is a class that handles IN checking. With a cache. It's a state machine, and
  * each checking using the contains() method returns both the result of the IN check and the new state.
  */
trait Checker {
  def contains(value: AnyValue): (Option[Boolean], Checker)
}

class BuildUp(list: ListValue) extends Checker {
  val iterator: util.Iterator[AnyValue] = list.iterator()
  assert(iterator.hasNext)
  private val cachedSet: mutable.Set[AnyValue] = new mutable.HashSet[AnyValue]
  override def contains(value: AnyValue): (Option[Boolean], Checker) = {
    if (value eq Values.NO_VALUE) (None, this)
    else {
      if (cachedSet.contains(value))
        (Some(true), this)
      else
        checkAndBuildUpCache(value)
    }
  }

  private def checkAndBuildUpCache(value: AnyValue): (Option[Boolean], Checker) = {
    var foundMatch = Equality.FALSE
    while (iterator.hasNext && foundMatch != Equality.TRUE) {
      val nextValue = iterator.next()
      if (nextValue eq Values.NO_VALUE) {
        foundMatch = Equality.UNDEFINED
      } else {
        cachedSet.add(nextValue)
        val areEqual = nextValue.ternaryEquals(value)
        if ((areEqual eq Equality.UNDEFINED) || (areEqual eq Equality.TRUE)) {
          foundMatch = areEqual
        }
      }
    }
    if (cachedSet.isEmpty) {
      (None, NullListChecker)
    } else {
      val falseResult = if (foundMatch == Equality.UNDEFINED) None else Some(false)
      val nextState = if (iterator.hasNext) this else new SetChecker(cachedSet, falseResult)
      val result = if (foundMatch == Equality.TRUE) Some(true) else falseResult

      (result, nextState)
    }
  }
}

case object AlwaysFalseChecker extends Checker {
  override def contains(value: AnyValue): (Option[Boolean], Checker) = (Some(false), this)
}

case object NullListChecker extends Checker {
  override def contains(value: AnyValue): (Option[Boolean], Checker) = (None, this)
}

// This is the final form for this cache.
class SetChecker(cachedSet: mutable.Set[AnyValue], falseResult: Option[Boolean]) extends Checker {

  assert(cachedSet.nonEmpty)

  override def contains(value: AnyValue): (Option[Boolean], Checker) = {
    if (value eq Values.NO_VALUE)
      (None, this)
    else {
      val exists = cachedSet.contains(value)
      val result = if (exists) Some(true) else falseResult
      (result, this)
    }
  }
}
