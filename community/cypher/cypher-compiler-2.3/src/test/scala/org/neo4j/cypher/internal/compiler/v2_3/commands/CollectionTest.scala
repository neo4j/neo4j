/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{CoercedPredicate, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite


class CollectionTest extends CypherFunSuite {

  test("any") {
    Seq() any false
    Seq(true) any true
    Seq(false) any false
    Seq(null) any null
    Seq(null, true) any true
    Seq(null, false) any null
    Seq(false, null) any null
    Seq(true, null) any true
  }

  test("all") {
    Seq() all true
    Seq(true) all true
    Seq(false) all false
    Seq(null) all null
    Seq(null, true) all null
    Seq(null, false) all false
    Seq(false, null) all false
    Seq(true, null) all null
  }

  test("single") {
    Seq() single false
    Seq(true) single true
    Seq(false) single false
    Seq(null) single null
    Seq(null, true) single null
    Seq(null, false) single null
    Seq(false, null) single null
    Seq(true, null) single null
    Seq(true, false) single true
    Seq(false, true) single true
    Seq(true, true) single false
    Seq(false, true, true) single false
    Seq(false, true, false) single true
    Seq(false, true, null) single null
  }

  test("none") {
    Seq() none true
    Seq(true) none false
    Seq(false) none true
    Seq(null) none null
    Seq(null, true) none false
    Seq(null, false) none null
    Seq(false, null) none null
    Seq(true, null) none false
    Seq(true, false) none false
    Seq(false, true) none false
    Seq(true, true) none false
    Seq(false, true, true) none false
    Seq(false, true, false) none false
    Seq(false, true, null) none false
  }

  implicit class Check(values: Seq[_]) {

    def any(expected: Any) = check(expected, AnyInCollection.apply)

    def all(expected: Any) = check(expected, AllInCollection.apply)

    def single(expected: Any) = check(expected, SingleInCollection.apply)

    def none(expected: Any) = check(expected, NoneInCollection.apply)

    private def check(expected: Any,
                      collectionFunction: (Expression, String, Predicate) => InCollection) {
      val function = collectionFunction(Literal(values), "x", CoercedPredicate(Identifier("x")))
      val result = function(ExecutionContext.empty)(QueryStateHelper.empty)
      result should equal(expected)
    }
  }
}
