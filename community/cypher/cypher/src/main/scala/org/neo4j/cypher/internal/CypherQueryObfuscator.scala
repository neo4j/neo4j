/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.values.storable.Values.utf8Value
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class CypherQueryObfuscator(state: ObfuscationMetadata) extends QueryObfuscator {

  override def obfuscateText(rawQueryText: String): String =
    if (state.sensitiveLiteralOffsets.isEmpty)
      rawQueryText
    else {
      val sb = new mutable.StringBuilder()
      var i = 0
      for (literalOffset <- state.sensitiveLiteralOffsets) {
        val start = literalOffset.start
        if (start >= rawQueryText.length || start < i) {
          throw new IllegalStateException(s"Literal offset out of bounds: $literalOffset.")
        }

        sb.append(rawQueryText.substring(i, start))
        sb.append(CypherQueryObfuscator.OBFUSCATED_LITERAL)
        i = start + literalOffset.length.getOrElse(literalStringLength(rawQueryText, start))
      }
      if (i < rawQueryText.length) {
        sb.append(rawQueryText.substring(i))
      }
      sb.toString
    }

  override def obfuscateParameters(rawQueryParameters: MapValue): MapValue =
    if (state.sensitiveParameterNames.isEmpty)
      rawQueryParameters
    else {
      var params = rawQueryParameters
      for (p <- state.sensitiveParameterNames)
        params = params.updatedWith(p, CypherQueryObfuscator.OBFUSCATED)
      params
    }

  // This should no longer be necessary, we should always know the length ahead of getting here.
  // However, at this point in time there is still one code-path leading to an unknown length and at the
  // moment of writing this I didn't have the guts to remove this method.
  private def literalStringLength(rawQueryText: String, fromIndex: Int): Int = {
    val first = rawQueryText.charAt(fromIndex)
    if (first == '"' || first == '\'') {
      // This literal starts with a quote so it must be a string-literal
      var lastCharacterIndex = fromIndex
      while (lastCharacterIndex < rawQueryText.length - 1) {
        if (rawQueryText(lastCharacterIndex) != '\\' && rawQueryText(lastCharacterIndex + 1) == first) {
          return lastCharacterIndex - fromIndex + 2
        }
        lastCharacterIndex += 1
      }
      throw new IllegalStateException("Expected to find closing quote.")
    } else {
      // this shouldn't happen but let's err on the side of caution and obfuscate the rest of the query
      rawQueryText.length - fromIndex
    }
  }

}

object CypherQueryObfuscator {
  private val OBFUSCATED_LITERAL = "******"
  private val OBFUSCATED = utf8Value(OBFUSCATED_LITERAL)

  def apply(obfuscationMetadata: ObfuscationMetadata): QueryObfuscator =
    if (obfuscationMetadata.isEmpty) QueryObfuscator.PASSTHROUGH else new CypherQueryObfuscator(obfuscationMetadata)
}
