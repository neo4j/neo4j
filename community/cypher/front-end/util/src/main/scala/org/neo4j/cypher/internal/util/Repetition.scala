/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.util

case class Repetition(min: Int, max: UpperBound)

sealed trait UpperBound {
  def isGreaterThan(count: Int): Boolean
}

object UpperBound {

  case object Unlimited extends UpperBound {
    override def isGreaterThan(count: Int): Boolean = true
  }

  case class Limited(n: Int) extends UpperBound {
    override def isGreaterThan(count: Int): Boolean = count < n
  }
}
