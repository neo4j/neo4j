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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.expressions.LiteralExtractor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

class ParameterLiteralExtractor extends LiteralExtractor {
  private val stack = new java.util.ArrayDeque[Writer]()
  stack.push(new NormalWriter)

  def value: AnyValue = {
    require(stack.size() == 1)
    stack.peek().value
  }
  override def writeLong(value: Long): Unit = {
    write(longValue(value))
  }
  override def writeBoolean(value: Boolean): Unit = {
    write(Values.booleanValue(value))
  }

  override def writeNull(): Unit = {
    write(NO_VALUE)
  }

  override def writeString(value: String): Unit = {
    write(stringValue(value))
  }

  override def writeDouble(value: Double): Unit = {
    write(doubleValue(value))
  }

  override def beginList(size: Int): Unit = {
    stack.push(new ListWriter(size))
  }

  override def endList(): Unit = {
    require(!stack.isEmpty)
    write(stack.pop().value)
  }

  override def writeByteArray(value: Array[Byte]): Unit = write(byteArray(value))

  private def write(anyValue: AnyValue): Unit = {
    require(!stack.isEmpty)
    val current = stack.peek()
    current.write(anyValue)
  }
}

sealed trait Writer {
  def write(value: AnyValue): Unit
  def value: AnyValue
}

class NormalWriter extends Writer {
  private var _value: AnyValue = _
  override def write(value: AnyValue): Unit = {
    _value = value
  }
  override def value: AnyValue = _value
}

class ListWriter(size: Int) extends Writer {
  private val array = new Array[AnyValue](size)
  private var index = 0
  override def write(value: AnyValue): Unit = {
    array(index) = value
    index += 1
  }
  override def value: AnyValue = {
    VirtualValues.list(array:_*)
  }
}
