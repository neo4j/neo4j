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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.MutableMaps
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Add
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class CommandTest extends ShowCommandTestBase {

  test("`extractNames` should filter out duplicates from list of strings") {
    // Given
    val names = List("foo", "bar", "baz", "bar", "foo")

    // When
    val result = Command.extractNames(Left(names), queryState, initialCypherRow)

    // Then
    result should have size 3
    result should contain theSameElementsAs List("foo", "bar", "baz")
  }

  test("`extractNames` should get exception on non string or list of string expression") {
    // Given
    val expression = ParameterFromSlot(0, "name")
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(Values.TRUE))

    // Then
    the[ParameterWrongTypeException] thrownBy {
      Command.extractNames(Right(expression), queryStateWithParams, initialCypherRow)
    } should have message "Expected a string or a list of strings, but got: Boolean('true')"
  }

  test("`extractNames` should get exception on list of non string expression") {
    // Given
    val expression = ParameterFromSlot(0, "name")
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(VirtualValues.list(Values.TRUE)))

    // Then
    the[ParameterWrongTypeException] thrownBy {
      Command.extractNames(Right(expression), queryStateWithParams, initialCypherRow)
    } should have message "Expected a string, but got: Boolean('true')"
  }

  test("`extractNames` should return single string expression") {
    // Given
    val expression = ParameterFromSlot(0, "name")
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(Values.stringValue("Hello!")))

    // When
    val result = Command.extractNames(Right(expression), queryStateWithParams, initialCypherRow)

    // Then
    result should have size 1
    result.head should be("Hello!")
  }

  test("`extractNames` should filter out duplicates from list of strings expression") {
    // Given
    val expression = ParameterFromSlot(0, "name")
    val paramValue = VirtualValues.list(
      Values.stringValue("Hello!"),
      Values.stringValue("Hi!"),
      Values.stringValue("Hello!"),
      Values.stringValue("Goodbye!")
    )
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(paramValue))

    // When
    val result = Command.extractNames(Right(expression), queryStateWithParams, initialCypherRow)

    // Then
    result should have size 3
    result should contain theSameElementsAs List("Hello!", "Hi!", "Goodbye!")
  }

  test("`extractNames` should handle complicated expression") {
    // Given
    val paramExpression = ParameterFromSlot(0, "name1")
    val paramValue = Values.stringValue("Hello!")
    val queryStateWithParams =
      QueryStateHelper.emptyWith(query = ctx, params = Array(paramValue))

    val variableExpression = Variable("name2")
    val expressionCypherRow = CypherRow.apply(MutableMaps.create("name2" -> Values.stringValue("Goodbye!")))

    val stringConcatExpression = Add(Literal(Values.stringValue("Hi")), Literal(Values.stringValue("!")))
    val listExpression = ListLiteral(paramExpression, variableExpression, stringConcatExpression)

    // When
    val result = Command.extractNames(Right(listExpression), queryStateWithParams, expressionCypherRow)

    // Then
    result should have size 3
    result should contain theSameElementsAs List("Hello!", "Goodbye!", "Hi!")
  }

}
