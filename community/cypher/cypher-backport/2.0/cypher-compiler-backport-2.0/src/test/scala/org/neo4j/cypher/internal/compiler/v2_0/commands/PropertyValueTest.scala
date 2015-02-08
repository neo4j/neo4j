/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions._
import values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryStateHelper
import org.scalatest.Assertions
import org.junit.Test

class PropertyValueTest extends Assertions {
  @Test def nullNodeShouldGiveNullProperty() {
    val p = Property(Identifier("identifier"), PropertyKey("property"))
    val ctx = ExecutionContext.from("identifier" -> null)
    val state = QueryStateHelper.empty

    assert(p(ctx)(state) === null)
  }

  @Test def nonExistentPropertyShouldEvaluateToNull() {
    val p = Property(Identifier("identifier"), PropertyKey("nonExistent"))
    val ctx = ExecutionContext.from("identifier" -> Map("property" -> 42))
    val state = QueryStateHelper.empty

    assert(p(ctx)(state) === null)
  }
}
