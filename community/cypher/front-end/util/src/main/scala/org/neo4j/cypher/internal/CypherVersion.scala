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
package org.neo4j.cypher.internal

/**
 * Cypher version.
 * Related to org.neo4j.kernel.api.CypherScope and org.neo4j.cypher.internal.options.CypherVersion.
 */
sealed trait CypherVersion {
  def name: String
  def experimental: Boolean
}

object CypherVersion {

  case object Cypher5 extends CypherVersion {
    override def name: String = "5"
    override def experimental: Boolean = false
  }

  case object Cypher6 extends CypherVersion {
    override def name: String = "6"
    override def experimental: Boolean = true
  }
  val Default: CypherVersion = Cypher5
  val All: Set[CypherVersion] = Set(Cypher5, Cypher6)
}
