/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.{AnyValue, Equality, VirtualValue}

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

  // If we don't return true, this is what we will return. If the collection contains any nulls, we'll return None,
  // else we return Some(false).
  private var falseResult: Option[Boolean] = Some(false)

  override def contains(value: AnyValue): (Option[Boolean], Checker) = {
    if (value == Values.NO_VALUE) (None, this)
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

      if (nextValue == Values.NO_VALUE) {
        falseResult = None
      } else {
        cachedSet.add(nextValue)
        //we will get UNDEFINED if we ternart compare different types but if we get an UNDEFINED when comparing two values
        //of the same type it means the whole result must be undefined, e.g. [1,2] IN [[null,2]]
        foundMatch = (nextValue, value) match {
          case (v1: Value, v2: Value) if v1.valueGroup() == v2.valueGroup() => v1.ternaryEquals(v2)
          case (v1: VirtualValue, v2: VirtualValue) if v1.valueGroup() == v2.valueGroup() => v1.ternaryEquals(v2)
          //For different types it is either TRUE or FALSE, 'foo' in [1,2] => false
          case (v1, v2) => if (v1.equals(v2)) Equality.TRUE else Equality.FALSE
        }

        if (foundMatch == Equality.UNDEFINED) {
          falseResult = None
          foundMatch = Equality.FALSE
        }
      }
    }
    if (cachedSet.isEmpty) {
      (None, NullListChecker)
    } else {
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
    if (value == Values.NO_VALUE)
      (None, this)
    else {
      val exists = cachedSet.contains(value)
      val result = if (exists) Some(true) else falseResult
      (result, this)
    }
  }
}
