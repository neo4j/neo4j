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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedProperty
import org.scalatest.Assertions

class LiteralMapTest extends Assertions {
  @Test
  def should_present_all_child_expressions() {
    val x = Identifier("x")
    // given
    val propX = Property(x, UnresolvedProperty("foo"))
    val count = CountStar()
    val literalMap = LiteralMap(Map("x" -> propX, "count" -> count))

    // when
    val subExpressions = literalMap.arguments.toSet

    // then
    assert(subExpressions === Set(propX, count))
  }
}
