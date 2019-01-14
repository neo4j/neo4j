/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.util.v3_4.attribution

import org.neo4j.cypher.internal.util.v3_4.Unchangeable

import scala.collection.mutable.ArrayBuffer

trait Attribute[T] {

  private val array: ArrayBuffer[Unchangeable[T]] = new ArrayBuffer[Unchangeable[T]]()

  def set(id: Id, t: T): Unit = {
    val requiredSize = id.x + 1
    if (array.size < requiredSize)
      resizeArray(requiredSize)
    array(id.x).value = t
  }

  def get(id: Id): T = {
    array(id.x).value
  }

  def isDefinedAt(id: Id): Boolean = {
    array.size > id.x && array(id.x).hasValue
  }

  def getOrElse(id: Id, other: => T): T = {
    if (isDefinedAt(id)) get(id) else other
  }

  def iterator: Iterator[(Id, T)] = new Iterator[(Id, T)]() {
    private var currentId = -1
    private var nextTup: (Id, T) = _

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

    override def hasNext = {
      if (currentId >= array.size)
        false
      else {
        if (nextTup == null) {
          fetchNext()
        }
        nextTup != null
      }
    }

    override def next() = {
      if (hasNext) {
        val res = nextTup
        nextTup = null
        res
      } else {
        throw new NoSuchElementException
      }
    }
  }

  def size = iterator.size

  def apply(id: Id): T = get(id)

  def copy(from:Id, to:Id): Unit = {
    if(isDefinedAt(from))
      set(to, get(from))
  }

  def mapTo[U](otherAttribute: Attribute[U], f: T => U): Unit = {
    var i = 0
    while(i < array.size) {
      val id = Id(i)
      if(isDefinedAt(id)) {
        otherAttribute.set(id, f(this.get(id)))
      }
      i = i + 1
    }
  }

  override def toString(): String = {
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
  * This class encapsulates attributes and allows to copy them from one ID to another without having explicit
  * read or write access. This allows rewriters to set some attributes manually on a new ID, but copying
  * others over from an old id.
  * @param idGen the IdGen used to provide new IDs
  * @param attributes the attributes encapsulated
  */
case class Attributes(idGen: IdGen, private val attributes: Attribute[_]*) {
  def copy(from: Id): IdGen = new IdGen {
    override def id(): Id = {
      val to = idGen.id()
      for (a <- attributes) {
        a.copy(from, to)
      }
      to
    }
  }

  def withAlso(attributes: Attribute[_]*) : Attributes = {
    Attributes(this.idGen, this.attributes ++ attributes: _*)
  }
}
