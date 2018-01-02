/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.commands.values

import org.neo4j.cypher.internal.compiler.v2_3.spi.{QueryContext, TokenContext}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

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
  def getOptIdForName(name: String, tokenContext: TokenContext): Option[Int] = m.get(name)

  def getIdForNameOrFail(name: String, tokenContext: TokenContext): Int = m(name)

  def getOrCreateIdForName(name: String, queryContext: QueryContext): Int = throw new UnsupportedOperationException
}
