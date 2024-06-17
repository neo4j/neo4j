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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray
import org.neo4j.memory.Measurable
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValueWriter
import org.neo4j.values.Equality
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueRepresentation
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import scala.collection.mutable
import scala.runtime.ScalaRunTime

object CypherRow {
  def empty: CypherRow = apply()

  def from(x: (String, AnyValue)*): CypherRow = {
    val context = empty
    context.set(x)
    context
  }

  def apply(m: mutable.Map[String, AnyValue] = MutableMaps.empty): MapCypherRow = new MapCypherRow(m, null)
}

case class ResourceLinenumber(filename: String, linenumber: Long, last: Boolean = false) extends AnyValue {
  override protected def equalTo(other: Any): Boolean = ScalaRunTime.equals(other)
  override protected def computeHash(): Int = ScalaRunTime._hashCode(ResourceLinenumber.this)
  override def writeTo[E <: Exception](writer: AnyValueWriter[E]): Unit = throw new UnsupportedOperationException()
  override def ternaryEquals(other: AnyValue): Equality = throw new UnsupportedOperationException()
  override def map[T](mapper: ValueMapper[T]): T = throw new UnsupportedOperationException()
  override def getTypeName: String = "ResourceLinenumber"

  override def estimatedHeapUsage(): Long =
    ResourceLinenumber.SHALLOW_SIZE // NOTE: The filename string is expected to be repeated so we do not count it here
  override def valueRepresentation(): ValueRepresentation = ValueRepresentation.UNKNOWN
}

object ResourceLinenumber {
  final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[ResourceLinenumber])
}

trait CypherRow extends ReadWriteRow with Measurable {

  @deprecated("We shouldn't check for the existence of variables at runtime", since = "4.0")
  def containsName(name: String): Boolean

  @deprecated("We shouldn't check for the existence of variables at runtime", since = "4.1")
  def numberOfColumns: Int

  def createClone(): CypherRow

  def copyWith(key: String, value: AnyValue): CypherRow
  def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow

  def copyWith(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow
  def copyWith(newEntries: collection.Seq[(String, AnyValue)]): CypherRow

  /** Create a copy of this row, with all values transformed by the given function (including cached values)  */
  def copyMapped(func: AnyValue => AnyValue): CypherRow

  def isNull(key: String): Boolean

  /**
   * Reduce size of row, by removing unused data, if possible.
   */
  def compact(): Unit = {}

  /**
   * This is a hack to avoid some of the worst heap over estimation of slotted runtime.
   *
   * @param previous another row that has the same slot configuration as this row
   *                 and is assumed to remain in memory for as long as this row does
   *                 or null
   */
  def deduplicatedEstimatedHeapUsage(previous: CypherRow): Long = estimatedHeapUsage()
}

object MapCypherRow {
  final private val SHALLOW_SIZE_OF_MUTABLE_MAP = shallowSizeOfInstance(classOf[mutable.OpenHashMap[_, _]])
  final private val SHALLOW_SIZE = shallowSizeOfInstance(classOf[MapCypherRow])

  final private val INITAL_SIZE_OF_MUTABLE_MAP =
    SHALLOW_SIZE_OF_MUTABLE_MAP + shallowSizeOfObjectArray(8) // OpenHashMap initial size 8
}

class MapCypherRow(
  private val m: mutable.Map[String, AnyValue],
  private var cachedProperties: mutable.Map[ASTCachedProperty.RuntimeKey, Value] = null
) extends CypherRow {

  private var linenumber: Option[ResourceLinenumber] = None

  def setLinenumber(line: Option[ResourceLinenumber]): Unit = {
    linenumber = line
  }

  // Used to copy the linenumber when copying or merging a row where we don't want to overwrite it
  def setLinenumberIfEmpty(line: Option[ResourceLinenumber]): Unit = linenumber match {
    case None => linenumber = line
    case _    =>
  }

  override def getLinenumber: Option[ResourceLinenumber] = linenumber

  override def copyAllFrom(input: ReadableRow): Unit = fail()

  override def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit = fail()

  override def copyLongsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, count: Int): Unit = fail()

  override def copyRefsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit = fail()

  override def copyFromOffset(
    input: ReadableRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit = fail()

  def remove(name: String): Option[AnyValue] = m.remove(name)
  // used for testing
  def toMap: Map[String, AnyValue] = m.toMap

  override def getByName(name: String): AnyValue =
    m.getOrElse(name, throw new NotFoundException(s"Unknown variable `$name`."))
  override def containsName(name: String): Boolean = m.contains(name)
  override def numberOfColumns: Int = m.size

  override def setLongAt(offset: Int, value: Long): Unit = fail()
  override def getLongAt(offset: Int): Long = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()
  override def getRefAt(offset: Int): AnyValue = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a map context as a slotted context")

  override def mergeWith(other: ReadableRow, entityById: EntityById, checkNullability: Boolean = true): Unit =
    other match {
      case otherMapCtx: MapCypherRow =>
        m ++= otherMapCtx.m
        if (otherMapCtx.cachedProperties != null) {
          if (cachedProperties == null) {
            cachedProperties = otherMapCtx.cachedProperties.clone()
          } else {
            cachedProperties ++= otherMapCtx.cachedProperties
          }
        } else {
          // otherMapCtx.cachedProperties is null so do nothing
        }
        setLinenumberIfEmpty(otherMapCtx.getLinenumber)
      case _ => fail()
    }

  override def set(newEntries: collection.Seq[(String, AnyValue)]): Unit =
    m ++= newEntries

  // This may seem silly but it has measurable impact in tight loops

  override def set(key: String, value: AnyValue): Unit =
    m.put(key, value)

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = {
    m.put(key1, value1)
    m.put(key2, value2)
  }

  override def set(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): Unit = {
    m.put(key1, value1)
    m.put(key2, value2)
    m.put(key3, value3)
  }

  override def copyWith(key: String, value: AnyValue): CypherRow = {
    val newMap = m.clone()
    newMap.put(key, value)
    cloneFromMap(newMap)
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    cloneFromMap(newMap)
  }

  override def copyWith(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    newMap.put(key3, value3)
    cloneFromMap(newMap)
  }

  override def copyWith(newEntries: collection.Seq[(String, AnyValue)]): CypherRow = {
    cloneFromMap(m.clone() ++ newEntries)
  }

  def copyMapped(func: AnyValue => AnyValue): CypherRow = {
    val newMap = m.map({ case (k, v) => k -> func(v) })
    val newCachedProperties =
      if (cachedProperties == null) null else cachedProperties.map({ case (k, v) => k -> func(v).asInstanceOf[Value] })
    val row = new MapCypherRow(newMap, newCachedProperties)
    row.setLinenumberIfEmpty(getLinenumber)
    row
  }

  override def createClone(): CypherRow = cloneFromMap(m.clone())

  override def isNull(key: String): Boolean =
    m.get(key) match {
      case Some(v) if v eq Values.NO_VALUE => true
      case _                               => false
    }

  override def setCachedProperty(key: ASTCachedProperty.RuntimeKey, value: Value): Unit = {
    if (cachedProperties == null) {
      cachedProperties = mutable.Map.empty
    }
    cachedProperties.put(key, value)
  }

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = fail()

  override def getCachedProperty(key: ASTCachedProperty.RuntimeKey): Value = {
    if (cachedProperties == null) {
      null
    } else {
      cachedProperties.getOrElse(key, null)
    }
  }

  override def getCachedPropertyAt(offset: Int): Value = fail()

  override def invalidateCachedProperties(): Unit = {
    cachedProperties = null
  }

  override def invalidateCachedNodeProperties(node: Long): Unit = {
    if (cachedProperties != null) {
      cachedProperties.keys.filter(cnp =>
        getByName(cnp.entityName) match {
          case n: VirtualNodeValue => n.id() == node
          case _                   => false
        }
      ).foreach(cnp => setCachedProperty(cnp, null))
    }
  }

  override def invalidateCachedRelationshipProperties(rel: Long): Unit = {
    if (cachedProperties != null) {
      cachedProperties.keys.filter(cnp =>
        getByName(cnp.entityName) match {
          case r: VirtualRelationshipValue => r.id() == rel
          case _                           => false
        }
      ).foreach(cnp => setCachedProperty(cnp, null))
    }
  }

  override def estimatedHeapUsage: Long = {
    var total = MapCypherRow.SHALLOW_SIZE + MapCypherRow.INITAL_SIZE_OF_MUTABLE_MAP
    val iterator = m.valuesIterator
    while (iterator.hasNext) {
      val value = iterator.next()
      if (value != null) {
        total += value.estimatedHeapUsage()
      }
    }
    if (cachedProperties != null) {
      total += MapCypherRow.INITAL_SIZE_OF_MUTABLE_MAP
      val iterator = cachedProperties.valuesIterator
      while (iterator.hasNext) {
        val value = iterator.next()
        if (value != null) {
          total += value.estimatedHeapUsage()
        }
      }
    }
    total
  }

  private def cloneFromMap(newMap: mutable.Map[String, AnyValue]): CypherRow = {
    val newCachedProperties = if (cachedProperties == null) null else cachedProperties.clone()
    val map = new MapCypherRow(newMap, newCachedProperties)
    map.setLinenumberIfEmpty(getLinenumber)
    map
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MapCypherRow]

  override def equals(other: Any): Boolean = other match {
    case that: MapCypherRow =>
      (that canEqual this) &&
      m == that.m
    case _ => false
  }

  override def hashCode(): Int = m.hashCode()

  override def toString: String = s"MapExecutionContext(m=$m, cached=$cachedProperties)"
}
