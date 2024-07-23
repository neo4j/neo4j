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
package org.neo4j.cypher.internal.ast

sealed trait CreateIndexType {
  def command: String
  def nodeDescription: String
  def relDescription: String
  def allDescription: String
  def singlePropertyOnly: Boolean
}

case object BtreeCreateIndex extends CreateIndexType {
  override val command: String = "BTREE INDEX"
  override val nodeDescription: String = "btree node index"
  override val relDescription: String = "btree relationship index"
  override val allDescription: String = "btree indexes"
  override val singlePropertyOnly: Boolean = false
}

case object FulltextCreateIndex extends CreateIndexType {
  override val command: String = "FULLTEXT INDEX"
  override val nodeDescription: String = "fulltext node index"
  override val relDescription: String = "fulltext relationship index"
  override val allDescription: String = "fulltext indexes"
  override val singlePropertyOnly: Boolean = false
}

case object LookupCreateIndex extends CreateIndexType {
  override val command: String = "LOOKUP INDEX"
  override val nodeDescription: String = "node lookup index"
  override val relDescription: String = "relationship lookup index"
  override val allDescription: String = "token lookup index"
  override val singlePropertyOnly: Boolean = false
}

case object PointCreateIndex extends CreateIndexType {
  override val command: String = "POINT INDEX"
  override val nodeDescription: String = "point node index"
  override val relDescription: String = "point relationship index"
  override val allDescription: String = "point indexes"
  override val singlePropertyOnly: Boolean = true
}

case class RangeCreateIndex(fromDefault: Boolean) extends CreateIndexType {
  override val command: String = if (fromDefault) "INDEX" else "RANGE INDEX"
  override val nodeDescription: String = "range node property index"
  override val relDescription: String = "range relationship property index"
  override val allDescription: String = "range indexes"
  override val singlePropertyOnly: Boolean = false
}

case object TextCreateIndex extends CreateIndexType {
  override val command: String = "TEXT INDEX"
  override val nodeDescription: String = "text node index"
  override val relDescription: String = "text relationship index"
  override val allDescription: String = "text indexes"
  override val singlePropertyOnly: Boolean = true
}

case object VectorCreateIndex extends CreateIndexType {
  override val command: String = "VECTOR INDEX"
  override val nodeDescription: String = "vector node index"
  override val relDescription: String = "vector relationship index"
  override val allDescription: String = "vector indexes"
  override val singlePropertyOnly: Boolean = true
}
