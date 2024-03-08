/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.collection.immutable

import scala.collection.IterableFactory
import scala.collection.IterableFactoryDefaults
import scala.collection.immutable.AbstractSet
import scala.collection.immutable.StrictOptimizedSetOps
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * A wrapper around a java.util.LinkedHashSet, exposing it as an immutable Set.
 *
 * [[incl()]] and [[excl()]] are implemented by creating copies of the underlying mutable java collection.
 *
 * Inspired by scala.collection.immutable.ListSet and scala.collection.convert.JavaCollectionWrappers.JSetWrapper
 */
class ListSet[A](underlying: java.util.LinkedHashSet[A])
    extends AbstractSet[A]
    with StrictOptimizedSetOps[A, ListSet, ListSet[A]]
    with IterableFactoryDefaults[A, ListSet] {

  override protected[this] def className: String = "ListSet"

  override def size: Int = underlying.size

  override def isEmpty: Boolean = underlying.isEmpty

  override def knownSize: Int = if (underlying.isEmpty) 0 else super.knownSize

  override def incl(elem: A): ListSet[A] = {
    if (contains(elem)) {
      this
    } else {
      val newJava = new java.util.LinkedHashSet(underlying)
      newJava.add(elem)
      new ListSet(newJava)
    }
  }

  override def excl(elem: A): ListSet[A] = {
    val newJava = new java.util.LinkedHashSet(underlying)
    newJava.remove(elem)
    new ListSet(newJava)
  }

  override def contains(elem: A): Boolean = underlying.contains(elem)

  override def iterator: Iterator[A] = underlying.iterator().asScala

  override def iterableFactory: IterableFactory[ListSet] = ListSet

  /**
   * Eagerly compute hashCode. This will prevent StackOverflowErrors with deeply nested expressions with ListSets,
   * e.g. Ands.
   */
  override val hashCode: Int = super.hashCode()
}

/**
 * $factoryInfo
 *
 * @define Coll ListSet
 */
@SerialVersionUID(3L)
object ListSet extends IterableFactory[ListSet] {

  /**
   * The Builder defers building the $Coll until [[result()]] is called.
   * Thereby, the performance is much better than calling [[ListSet.incl()]] repeatedly:
   * The builder will only create one instance of an java.util.LinkedHashSet.
   */
  private class Builder[A] extends mutable.Builder[A, ListSet[A]] {
    private val javaSet = new java.util.LinkedHashSet[A]()

    override def clear(): Unit = {
      throw new UnsupportedOperationException()
    }

    override def addOne(elem: A): Builder.this.type = {
      javaSet.add(elem)
      this
    }

    override def result(): ListSet[A] = new ListSet(javaSet)
  }

  def from[E](it: scala.collection.IterableOnce[E]): ListSet[E] =
    it match {
      case ls: ListSet[E]         => ls
      case _ if it.knownSize == 0 => empty[E]
      case _                      => (newBuilder[E] ++= it).result()
    }

  private object EmptyListSet extends ListSet[Any](new java.util.LinkedHashSet[Any]()) {
    override def knownSize: Int = 0
  }

  def empty[A]: ListSet[A] = EmptyListSet.asInstanceOf[ListSet[A]]

  def newBuilder[A]: mutable.Builder[A, ListSet[A]] =
    new Builder[A]
}
