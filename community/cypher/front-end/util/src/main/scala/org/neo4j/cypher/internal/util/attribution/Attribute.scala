/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.attribution

import org.neo4j.cypher.internal.util.Unchangeable

import scala.collection.mutable.ArrayBuffer

trait Attribute[KEY, VALUE] {

  private val array: ArrayBuffer[Unchangeable[VALUE]] = new ArrayBuffer[Unchangeable[VALUE]]()

  def copyTo[T <: Attribute[KEY, VALUE]](to: T): T = {
    array.copyToBuffer(to.array)
    to
  }

  def set(id: Id, t: VALUE): Unit = {
    val requiredSize = id.x + 1
    if (array.size < requiredSize)
      resizeArray(requiredSize)
    array(id.x).value = t
  }

  def get(id: Id): VALUE = {
    array(id.x).value
  }

  def isDefinedAt(id: Id): Boolean = {
    array.size > id.x && id.x >= 0 && array(id.x).hasValue
  }

  def getOrElse(id: Id, other: => VALUE): VALUE = {
    if (isDefinedAt(id)) get(id) else other
  }

  def iterator: Iterator[(Id, VALUE)] = new Iterator[(Id, VALUE)]() {
    private var currentId = -1
    private var nextTup: (Id, VALUE) = _

    private def fetchNext(): Unit = {
      nextTup = null
      while (nextTup == null &&
        {currentId = currentId + 1; currentId} < array.size) {
        val c = array(currentId)
        if (c.hasValue) {
          nextTup = (Id(currentId), c.value)
        }
      }
    }

    override def hasNext: Boolean = {
      if (currentId >= array.size)
        false
      else {
        if (nextTup == null) {
          fetchNext()
        }
        nextTup != null
      }
    }

    override def next(): (Id, VALUE) = {
      if (hasNext) {
        val res = nextTup
        nextTup = null
        res
      } else {
        throw new NoSuchElementException
      }
    }
  }

  def size: Int = iterator.size

  def apply(id: Id): VALUE = get(id)

  def copy(from:Id, to:Id): Unit = {
    if(isDefinedAt(from))
      set(to, get(from))
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb ++= this.getClass.getSimpleName + "\n"
    for (i <- array.indices)
      sb ++= s"$i : ${array(i)}\n"
    sb.result()
  }

  private def resizeArray(requiredSize: Int): Unit = {
    while (array.size < requiredSize)
      array += new Unchangeable
  }
}

/**
 * Mixin trait to override behavior of `get`. Does not alter behavior of other methods.
 */
trait Default[KEY <: Identifiable, VALUE] extends Attribute[KEY, VALUE] {

  protected def defaultValue: VALUE

  override def get(id: Id): VALUE = if (isDefinedAt(id)) super.get(id) else defaultValue
}

/**
 * Use this to signal that an Attribute is not required to be set for all KEYs
 * @param defaultValue the default value if the Attribute is not set.
 */
abstract class PartialAttribute[KEY <: Identifiable, VALUE](override val defaultValue: VALUE) extends Default[KEY, VALUE]

/**
 * This class encapsulates attributes and allows to copy them from one ID to another without having explicit
 * read or write access. This allows rewriters to set some attributes manually on a new ID, but copying
 * others over from an old id.
 * @param idGen the IdGen used to provide new IDs
 * @param attributes the attributes encapsulated
 */
case class Attributes[KEY <: Identifiable](idGen: IdGen, private val attributes: Attribute[KEY, _]*) {
  def copy(from: Id): IdGen = new IdGen {
    override def id(): Id = {
      val to = idGen.id()
      for (a <- attributes) {
        a.copy(from, to)
      }
      to
    }
  }

  def withAlso(attributes: Attribute[KEY, _]*) : Attributes[KEY] = {
    Attributes(this.idGen, this.attributes ++ attributes: _*)
  }
}
