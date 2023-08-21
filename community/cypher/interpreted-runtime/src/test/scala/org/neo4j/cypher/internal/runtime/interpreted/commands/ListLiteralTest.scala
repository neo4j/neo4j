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
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values.FALSE
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.TRUE
import org.neo4j.values.virtual.VirtualValues

class ListLiteralTest extends CypherFunSuite {

  test("any") {
    Seq[Any]() any FALSE
    Seq[Any](true) any TRUE
    Seq[Any](false) any FALSE
    Seq[Any](null) any NO_VALUE
    Seq[Any](null, true) any TRUE
    Seq[Any](null, false) any NO_VALUE
    Seq[Any](false, null) any NO_VALUE
    Seq[Any](true, null) any TRUE
  }

  test("all") {
    Seq[Any]() all TRUE
    Seq[Any](true) all TRUE
    Seq[Any](false) all FALSE
    Seq[Any](null) all NO_VALUE
    Seq[Any](null, true) all NO_VALUE
    Seq[Any](null, false) all FALSE
    Seq[Any](false, null) all FALSE
    Seq[Any](true, null) all NO_VALUE
  }

  test("single") {
    Seq[Any]() single FALSE
    Seq[Any](true) single TRUE
    Seq[Any](false) single FALSE
    Seq[Any](null) single NO_VALUE
    Seq[Any](null, true) single NO_VALUE
    Seq[Any](null, false) single NO_VALUE
    Seq[Any](false, null) single NO_VALUE
    Seq[Any](true, null) single NO_VALUE
    Seq[Any](true, false) single TRUE
    Seq[Any](false, true) single TRUE
    Seq[Any](true, true) single FALSE
    Seq[Any](false, true, true) single FALSE
    Seq[Any](false, true, false) single TRUE
    Seq[Any](false, true, null) single NO_VALUE
  }

  test("none") {
    Seq[Any]() none TRUE
    Seq[Any](true) none FALSE
    Seq[Any](false) none TRUE
    Seq[Any](null) none NO_VALUE
    Seq[Any](null, true) none FALSE
    Seq[Any](null, false) none NO_VALUE
    Seq[Any](false, null) none NO_VALUE
    Seq[Any](true, null) none FALSE
    Seq[Any](true, false) none FALSE
    Seq[Any](false, true) none FALSE
    Seq[Any](true, true) none FALSE
    Seq[Any](false, true, true) none FALSE
    Seq[Any](false, true, false) none FALSE
    Seq[Any](false, true, null) none FALSE
  }

  implicit class Check(values: Seq[_]) {

    def any(expected: Any) = check(expected, AnyInList.apply)

    def all(expected: Any) = check(expected, AllInList.apply)

    def single(expected: Any) = check(expected, SingleInList.apply)

    def none(expected: Any) = check(expected, NoneInList.apply)

    private def check(expected: Any, collectionFunction: (Expression, String, Int, Predicate) => InList): Unit = {

      val function = collectionFunction(
        Literal(VirtualValues.list(values.map(ValueUtils.of): _*)),
        "x",
        0,
        CoercedPredicate(ExpressionVariable(0, "x"))
      )
      val result = function(CypherRow.empty, QueryStateHelper.emptyWith(expressionVariables = new Array(1)))
      result should equal(expected)
    }
  }
}
