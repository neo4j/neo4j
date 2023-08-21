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
package org.neo4j.cypher.internal.util.attribution

import org.neo4j.cypher.internal.util.RewritableUniversal

case class Id(x: Int) extends AnyVal with RewritableUniversal {

  override def dup(children: Seq[AnyRef]): this.type = {
    val newId = children.head.asInstanceOf[Int]
    if (newId != x) Id(newId).asInstanceOf[this.type]
    else this
  }
}

object Id {
  val INVALID_ID: Id = Id(-1)
}

/**
 * Generates IDs
 */
trait IdGen {

  /**
   * @return an ID
   */
  def id(): Id
}

/**
 * Generates IDs in sequence starting at `initialValue`
 */
class SequentialIdGen(initialValue: Int = 0) extends IdGen {
  private var i: Int = initialValue

  def id(): Id = {
    val id = Id(i)
    i += 1
    id
  }
}

/**
 * Generates only the given ID.
 */
case class SameId(id: Id) extends IdGen

/**
 * An entity that is defined by its ID.
 */
trait Identifiable {

  /**
   * @return the ID of the entity.
   */
  def id: Id
}
