/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.HasLabel
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class HasLabelTests extends CypherFunSuite {
  test("should_handle_null_values") {
    //given match n-[?]-m
    val predicate = HasLabel(Literal(null), KeyToken.Unresolved("Person", TokenType.Label))

    //when
    val ctx = ExecutionContext.empty
    val state = QueryStateHelper.empty

    //then
    predicate.isMatch(ctx, state) should equal(None)
  }
}
