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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Expression, Literal, Variable}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.{CoercedPredicate, Predicate}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{FALSE, NO_VALUE, TRUE}

class ListLiteralTest extends CypherFunSuite {

  test("any") {
    Seq() any FALSE
    Seq(true) any TRUE
    Seq(false) any FALSE
    Seq(null) any NO_VALUE
    Seq(null, true) any TRUE
    Seq(null, false) any NO_VALUE
    Seq(false, null) any NO_VALUE
    Seq(true, null) any TRUE
  }

  test("all") {
    Seq() all TRUE
    Seq(true) all TRUE
    Seq(false) all FALSE
    Seq(null) all NO_VALUE
    Seq(null, true) all NO_VALUE
    Seq(null, false) all FALSE
    Seq(false, null) all FALSE
    Seq(true, null) all NO_VALUE
  }

  test("single") {
    Seq() single FALSE
    Seq(true) single TRUE
    Seq(false) single FALSE
    Seq(null) single NO_VALUE
    Seq(null, true) single NO_VALUE
    Seq(null, false) single NO_VALUE
    Seq(false, null) single NO_VALUE
    Seq(true, null) single NO_VALUE
    Seq(true, false) single TRUE
    Seq(false, true) single TRUE
    Seq(true, true) single FALSE
    Seq(false, true, true) single FALSE
    Seq(false, true, false) single TRUE
    Seq(false, true, null) single NO_VALUE
  }

  test("none") {
    Seq() none TRUE
    Seq(true) none FALSE
    Seq(false) none TRUE
    Seq(null) none NO_VALUE
    Seq(null, true) none FALSE
    Seq(null, false) none NO_VALUE
    Seq(false, null) none NO_VALUE
    Seq(true, null) none FALSE
    Seq(true, false) none FALSE
    Seq(false, true) none FALSE
    Seq(true, true) none FALSE
    Seq(false, true, true) none FALSE
    Seq(false, true, false) none FALSE
    Seq(false, true, null) none FALSE
  }

  implicit class Check(values: Seq[_]) {

    def any(expected: Any) = check(expected, AnyInList.apply)

    def all(expected: Any) = check(expected, AllInList.apply)

    def single(expected: Any) = check(expected, SingleInList.apply)

    def none(expected: Any) = check(expected, NoneInList.apply)

    private def check(expected: Any,
                      collectionFunction: (Expression, String, Predicate) => InList) {
      val function = collectionFunction(Literal(values), "x", CoercedPredicate(Variable("x")))
      val result = function(ExecutionContext.empty)(QueryStateHelper.empty)
      result should equal(expected)
    }
  }
}
