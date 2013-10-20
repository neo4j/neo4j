/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class IdentifierTest extends Assertions {

  @Test
  def shouldDefineIdentifierDuringSemanticCheckWhenUndefined() {
    val token = DummyToken(0, 1)
    val identifier = Identifier("x", token)

    val result = identifier.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(1, result.errors.size)
    assertEquals(token, result.errors.head.token)
    assertTrue(result.state.symbol("x").isDefined)
    assertEquals(Set(AnyType()), result.state.symbolTypes("x"))
  }
}
