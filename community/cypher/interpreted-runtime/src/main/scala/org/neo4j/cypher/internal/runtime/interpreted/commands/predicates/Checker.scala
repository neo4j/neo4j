/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.Equality
import org.neo4j.values.SequenceValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

import java.util

/**
 * This is a class that handles IN checking. With a cache. It's a state machine, and
 * each checking using the contains() method returns both the result of the IN check and the new state.
 */
trait Checker {
  def contains(value: AnyValue): (Option[Boolean], Checker)
  def close(): Unit
}

class BuildUp(list: ListValue, memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE) extends Checker {
  val iterator: util.Iterator[AnyValue] = list.iterator()
  checkOnlyWhenAssertionsAreEnabled(iterator.hasNext)
  private val cachedSet = HeapTrackingCollections.newSet[AnyValue](memoryTracker)
  private var falseResult: Option[Boolean] = Some(false)

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
        falseResult = None
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
      val nextState = if (iterator.hasNext) this else new SetChecker(cachedSet, falseResult, Some(cachedSet))

      foundMatch match {
        case Equality.TRUE => (Some(true), nextState)
        case Equality.FALSE if (value.isInstanceOf[SequenceValue] || value.isInstanceOf[MapValue]) && falseResult.isDefined =>
          val undefinedEquality = cachedSet.stream().anyMatch(setValue => value.ternaryEquals(setValue) eq Equality.UNDEFINED)
          if (undefinedEquality) {
            (None, nextState)
          } else {
            (Some(false), nextState)
          }
        case Equality.FALSE => (falseResult, nextState)
        case Equality.UNDEFINED => (None, nextState)
      }
    }
  }

  override def close(): Unit = {
    cachedSet.close()
  }
}

case object AlwaysFalseChecker extends Checker {
  override def contains(value: AnyValue): (Option[Boolean], Checker) = (Some(false), this)

  override def close(): Unit = {}
}

case object NullListChecker extends Checker {
  override def contains(value: AnyValue): (Option[Boolean], Checker) = (None, this)
  override def close(): Unit = {}
}

/**
 * This is the final form of this cache.
 *
 * @param cachedSet cached values
 * @param falseResult return value if `cachedSet` does not match. Note, not valid for sequence values or map values.
 * @param resource resource
 */
class SetChecker(cachedSet: java.util.Set[AnyValue], falseResult: Option[Boolean], resource: Option[AutoCloseable] = None) extends Checker {

  checkOnlyWhenAssertionsAreEnabled(!cachedSet.isEmpty)

  override def contains(value: AnyValue): (Option[Boolean], Checker) = {
    if (value eq Values.NO_VALUE)
      (None, this)
    else if (cachedSet.contains(value)) {
      (Some(true), this)
    } else if (falseResult.isEmpty) {
      (falseResult, this)
    } else if (value.isInstanceOf[SequenceValue] || value.isInstanceOf[MapValue]) {
      val undefinedEquality = cachedSet.stream().anyMatch(setValue => value.ternaryEquals(setValue) eq Equality.UNDEFINED)
      if (undefinedEquality) {
        (None, this)
      } else {
        (Some(false), this)
      }
    } else {
      (falseResult, this)
    }
  }

  override def close(): Unit = resource.foreach(_.close())
}
