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
package org.neo4j.cypher.internal.compiler.v3_1.helpers

import java.util

/**
  * Simple wrapper for a java.util.Map which preserves the original list
  * while lazily converts to scala values if needed.
  *
  * @param inner the inner java map
  * @param converter converter from java values to scala values
  */
case class JavaMapWrapper(inner: java.util.Map[String,Any], converter: RuntimeScalaValueConverter) extends Map[String,Any] {

  override def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = {
    val newInner = new util.HashMap[String, Any](inner)
    newInner.put(kv._1, kv._2)
    copy(inner = newInner)
  }

  override def get(key: String): Option[Any] = Option(inner.get(key))


  override def iterator: Iterator[(String, Any)] = new Iterator[(String, Any)] {
    private val innerIterator = inner.entrySet().iterator();
    override def hasNext: Boolean = innerIterator.hasNext

    override def next(): (String, Any) = {
      val entry  = innerIterator.next()
      (entry.getKey, converter.asDeepScalaValue(entry.getValue))
    }
  }

  override def -(key: String): Map[String, Any] = {
    val newInner = new util.HashMap[String, Any](inner)
    newInner.remove(key)
    copy(inner = newInner)
  }
}
