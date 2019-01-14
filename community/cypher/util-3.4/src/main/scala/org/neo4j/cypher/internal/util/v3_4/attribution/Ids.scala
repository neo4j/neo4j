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

case class Id(x: Int) extends AnyVal

object Id {
  val INVALID_ID: Id = Id(-1)
}

trait IdGen {
  def id(): Id
}

class SequentialIdGen(initialValue: Int = 0) extends IdGen {
  private var i: Int = initialValue

  def id(): Id = {
    val id = Id(i)
    i += 1
    id
  }
}

case class SameId(id: Id) extends IdGen

