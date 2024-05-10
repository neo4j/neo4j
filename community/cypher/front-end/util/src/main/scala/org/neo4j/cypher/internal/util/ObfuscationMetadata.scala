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

final case class ObfuscationMetadata(
  sensitiveLiteralOffsets: Vector[LiteralOffset],
  sensitiveParameterNames: Set[String]
) {
  def isEmpty: Boolean = sensitiveLiteralOffsets.isEmpty && sensitiveParameterNames.isEmpty
}

object ObfuscationMetadata {
  def empty() = new ObfuscationMetadata(Vector.empty, Set.empty)
}

case class LiteralOffset(private val start: Int, length: Option[Int]) {
  def start(preParserOffset: Int): Int = start + preParserOffset
}
