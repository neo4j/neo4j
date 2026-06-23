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

/**
 * Obfuscation metadata collected during parsing, carrying two separate views of the literals to redact
 */
final case class ObfuscationMetadata(
  sensitiveLiteralOffsets: Vector[LiteralOffset],
  allLiteralOffsets: Vector[LiteralOffset],
  sensitiveParameterNames: Set[String]
) {

  def isEmpty: Boolean =
    sensitiveLiteralOffsets.isEmpty && allLiteralOffsets.isEmpty && sensitiveParameterNames.isEmpty

  // Constructed via the companion `apply` (not `new`), so both offset vectors are normalized — a future
  // switch to `new ObfuscationMetadata(...)` here would silently drop that normalization. See `normalize`.
  def merge(other: ObfuscationMetadata): ObfuscationMetadata = ObfuscationMetadata(
    sensitiveLiteralOffsets.appendedAll(other.sensitiveLiteralOffsets),
    allLiteralOffsets.appendedAll(other.allLiteralOffsets),
    sensitiveParameterNames.union(other.sensitiveParameterNames)
  )
}

object ObfuscationMetadata {

  def apply(
    sensitiveOffsets: Vector[LiteralOffset],
    allOffsets: Vector[LiteralOffset],
    params: Set[String]
  ): ObfuscationMetadata =
    new ObfuscationMetadata(normalize(sensitiveOffsets), normalize(allOffsets), params)

  /**
   * De-duplicate, sort by start, and keep at most one offset per start position. When several offsets share a
   * start, prefer a known length and keep the widest (fail closed — redact at least as much as any candidate
   * asked for); fall back to an unknown-length (`None`) offset only when no known length exists at that start.
   */
  private def normalize(offsets: Vector[LiteralOffset]): Vector[LiteralOffset] =
    offsets
      .groupBy(_.start(0))
      .toVector
      .map { case (_, group) =>
        val withKnownLength = group.filter(_.length.isDefined)
        if (withKnownLength.isEmpty) group.head
        else withKnownLength.maxBy(_.length.get)
      }
      .sortBy(_.start(0))

  def empty() = new ObfuscationMetadata(Vector.empty, Vector.empty, Set.empty)
}

/**
 * Position and length of obfuscated literals.
 *
 * @param start offset of the literal relative to the query string without preparser options
 * @line line number of the literal relative to the query string without preparser options
 * @param length length of literal in query string
 */
case class LiteralOffset(private val start: Int, private val line: Int, length: Option[Int]) {
  def start(preParserOffset: Int): Int = start + preParserOffset
  def line(preParserLineOffset: Int): Int = line + preParserLineOffset
}
