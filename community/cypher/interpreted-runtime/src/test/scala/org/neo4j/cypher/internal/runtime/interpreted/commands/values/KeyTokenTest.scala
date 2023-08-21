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
package org.neo4j.cypher.internal.runtime.interpreted.commands.values

import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class KeyTokenTest extends CypherFunSuite {

  test("should_resolve_unresolved") {
    // given
    val tokenType = MapKeyTokenType(Map("a" -> 1))
    val keyToken = KeyToken.Unresolved("a", tokenType)

    // when
    val result = keyToken.resolve(null)

    // then
    result should equal(KeyToken.Resolved("a", 1, tokenType))
  }

  test("should_not_resolve_resolved") {
    // given
    val tokenType = MapKeyTokenType(Map("a" -> 1))
    val keyToken = KeyToken.Resolved("a", 2, tokenType)

    // when
    val result = keyToken.resolve(null)

    // then
    result should equal(KeyToken.Resolved("a", 2, tokenType))
  }

  test("should_not_resolve_unknown") {
    // given
    val tokenType = MapKeyTokenType(Map("a" -> 1))
    val keyToken = KeyToken.Unresolved("b", tokenType)

    // when
    val result = keyToken.resolve(null)

    // then
    result should equal(KeyToken.Unresolved("b", tokenType))
  }
}

case class MapKeyTokenType(m: Map[String, Int]) extends TokenType {
  def getOptIdForName(name: String, tokenContext: ReadTokenContext): Option[Int] = m.get(name)

  def getIdForNameOrFail(name: String, tokenContext: ReadTokenContext): Int = m(name)

  def getOrCreateIdForName(name: String, queryContext: QueryContext): Int = throw new UnsupportedOperationException
}
