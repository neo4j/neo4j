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
package org.neo4j.cypher.internal.util

case class Repetition(min: Long, max: UpperBound) {
  def solvedString: String = s"{$min, ${max.solvedString}}"
}

sealed trait UpperBound {
  def isGreaterThan(count: Long): Boolean

  def limit: Option[Long]

  def solvedString: String
}

object UpperBound {

  def unlimited: UpperBound = Unlimited

  case object Unlimited extends UpperBound {
    override def isGreaterThan(count: Long): Boolean = true
    override def limit: Option[Long] = None
    override def solvedString: String = ""
  }

  case class Limited(n: Long) extends UpperBound {
    require(n > 0)
    override def isGreaterThan(count: Long): Boolean = count < n
    override def limit: Option[Long] = Some(n)
    override def solvedString: String = n.toString
  }
}
