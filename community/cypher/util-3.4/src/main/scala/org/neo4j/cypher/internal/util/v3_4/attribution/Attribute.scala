/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

  // TODO crappy perf?
  val array:ArrayBuffer[Unchangeable[T]] = new ArrayBuffer[Unchangeable[T]]()

  def set(id:Id, t:T): Unit = {
    val requiredSize = id.x + 1
    if (array.size < requiredSize) {
      while (array.size < requiredSize)
        array += new Unchangeable
      array(id.x).value = t
    } else {
      val prev = array(id.x)
      array(id.x).value = t
    }
  }

  def get(id:Id): T = {
    array(id.x).value
  }

  def copy(from:Id, to:Id): Unit = {
    set(to, get(from))
  }

  override def toString(): String = {
    val sb = new StringBuilder
    sb ++= this.getClass.getSimpleName + "\n"
    for ( i <- array.indices )
      sb ++= s"$i : ${array(i)}\n"
    sb.result()
  }
}