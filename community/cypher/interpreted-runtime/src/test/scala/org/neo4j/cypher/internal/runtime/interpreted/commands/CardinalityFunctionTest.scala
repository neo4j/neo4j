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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toListValue
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toStringValue
import org.neo4j.cypher.internal.runtime.PathImpl
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CardinalityFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.functionArgumentGqlException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualValues

class CardinalityFunctionTest extends InterpretedRuntimeTestSuite {

  test("cardinality can be used on lists") {
    val l = Seq(1, 2, 3)
    val m = CypherRow.from("l" -> l)
    val cardinalityFunction = CardinalityFunction(Variable("l"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(3))
  }

  test("cardinality returns zero for empty list") {
    val m = CypherRow.from("l" -> Seq.empty[Any])
    val cardinalityFunction = CardinalityFunction(Variable("l"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(0))
  }

  test("cardinality counts null element in list") {
    val m = CypherRow.from("l" -> Seq(Values.NO_VALUE, Values.NO_VALUE))
    val cardinalityFunction = CardinalityFunction(Variable("l"))
    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(2))
  }

  test("cardinality can be used on maps") {
    val map = VirtualValues.map(Array("a", "b"), Array(Values.longValue(1), Values.stringValue("foo")))
    val m = CypherRow.from("map" -> map)
    val cardinalityFunction = CardinalityFunction(Variable("map"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(2))
  }

  test("cardinality returns zero for empty map") {
    val map = VirtualValues.map(Array.empty[String], Array.empty)
    val m = CypherRow.from("map" -> map)
    val cardinalityFunction = CardinalityFunction(Variable("map"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(0))
  }

  test("cardinality counts keys of null fields in maps") {
    val map = VirtualValues.map(Array("a", "b"), Array(Values.longValue(1), Values.NO_VALUE))
    val m = CypherRow.from("map" -> map)
    val cardinalityFunction = CardinalityFunction(Variable("map"))
    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(2))
  }

  test("cardinality can be used on paths") {
    val p = PathImpl(mockNode(), mock[Relationship], mockNode())
    val m = CypherRow.from("p" -> ValueUtils.fromPath(p))
    val cardinalityFunction = CardinalityFunction(Variable("p"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(longValue(3))
  }

  test("cardinality returns null for null input") {
    val m = CypherRow.from("x" -> Values.NO_VALUE)
    val cardinalityFunction = CardinalityFunction(Variable("x"))

    cardinalityFunction(m, QueryStateHelper.empty) should equal(Values.NO_VALUE)
  }

  test("cardinality cannot be used on strings") {
    val s = "hello"
    val m = CypherRow.from("s" -> s)
    val cardinalityFunction = CardinalityFunction(Variable("s"))

    val e = intercept[CypherTypeException](cardinalityFunction.apply(m, QueryStateHelper.empty))
    e should be(functionArgumentGqlException(
      "Invalid input for function 'cardinality()': Expected a Map, List or Path, got: String(\"hello\")",
      "cardinality()",
      "Expected the value \"hello\" to be of type MAP, LIST<ANY> or PATH, but was of type STRING NOT NULL."
    ))
  }

  test("cardinality cannot be used on integers") {
    val m = CypherRow.from("n" -> longValue(42))
    val cardinalityFunction = CardinalityFunction(Variable("n"))

    val e = intercept[CypherTypeException](cardinalityFunction.apply(m, QueryStateHelper.empty))
    e should be(functionArgumentGqlException(
      "Invalid input for function 'cardinality()': Expected a Map, List or Path, got: Long(42)",
      "cardinality()",
      "Expected the value 42 to be of type MAP, LIST<ANY> or PATH, but was of type INTEGER NOT NULL."
    ))
  }

  private def mockNode() = {
    val node = mock[Node]
    when(node.getElementId).thenReturn("dummy")
    node
  }
}
