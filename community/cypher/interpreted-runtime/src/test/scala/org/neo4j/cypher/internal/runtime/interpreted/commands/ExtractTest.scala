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
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toListValue
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExtractFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SizeFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.virtual.VirtualValues.list

class ExtractTest extends CypherFunSuite {

  test("canReturnSomethingFromAnIterable") {
    val l = Seq("x", "xxx", "xx")
    val expression = SizeFunction(ExpressionVariable(0, "n"))
    val collection = Variable("l")
    val m = CypherRow.from("l" -> l)

    val extract = ExtractFunction(collection, "n", 0, expression)

    extract.apply(m, QueryStateHelper.emptyWith(expressionVariables = new Array(1))) should equal(list(
      intValue(1),
      intValue(3),
      intValue(2)
    ))
  }
}
