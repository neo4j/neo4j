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
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Add
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.LengthFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ReduceFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SizeFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.longValue

class ReduceTest extends CypherFunSuite {

  test("canReturnSomethingFromAnIterable") {
    val l = Seq("x", "xxx", "xx")
    val expression = Add(ExpressionVariable(0, "acc"), SizeFunction(ExpressionVariable(1, "n")))
    val collection = Variable("l")
    val m = CypherRow.from("l" -> l)
    val s = QueryStateHelper.emptyWith(expressionVariables = new Array(2))

    val reduce = ReduceFunction(collection, "n", 1, expression, "acc", 0, literal(0))

    reduce.apply(m, s) should equal(longValue(6))
  }

  test("returns_null_from_null_collection") {
    val expression = Add(ExpressionVariable(0, "acc"), LengthFunction(ExpressionVariable(1, "n")))
    val collection = literal(NO_VALUE)
    val m = CypherRow.empty
    val s = QueryStateHelper.emptyWith(expressionVariables = new Array(2))

    val reduce = ReduceFunction(collection, "n", 1, expression, "acc", 0, literal(0))

    reduce(m, s) should equal(NO_VALUE)
  }
}
