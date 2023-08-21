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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toMapValue
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Property
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE

class PropertyValueComparisonTest extends CypherFunSuite {

  private val expectedNull = NO_VALUE

  test("nullNodeShouldGiveNullProperty") {
    val p = Property(Variable("variable"), PropertyKey("property"))
    val ctx = CypherRow.from("variable" -> NO_VALUE)
    val state = QueryStateHelper.empty

    p(ctx, state) should equal(expectedNull)
  }

  test("nonExistentPropertyShouldEvaluateToNull") {
    val p = Property(Variable("variable"), PropertyKey("nonExistent"))
    val ctx = CypherRow.from("variable" -> Map("property" -> 42))
    val state = QueryStateHelper.empty

    p(ctx, state) should equal(expectedNull)
  }
}
