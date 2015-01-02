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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.Assertions
import org.junit.Test
import org.junit.runners.Parameterized.Parameters
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Identifier, Literal, Expression}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryStateHelper


@RunWith(value = classOf[Parameterized])
class CollectionTest(expectedResult: Any,
                     collectionFunctionName: String,
                     collectionFunction: CollectionTest.CollectionFunction,
                     values: Seq[Boolean]) extends Assertions {

  @Test def test() {
    val function = collectionFunction(Literal(values), "x", CoercedPredicate(Identifier("x")))
    val result = function(ExecutionContext.empty)(QueryStateHelper.empty)
    assert(expectedResult === result)
  }
}

object CollectionTest {

  type CollectionFunction = (Expression, String, Predicate) => InCollection

  @Parameters(name = "{1} in {3} => {0}")
  def parameters: java.util.Collection[Array[Any]] = {
    val list = new java.util.ArrayList[Array[Any]]()

    def add(expectedResult: Any,
            collectionFunctionName: String,
            collectionFunction: CollectionFunction,
            values: Seq[Any]) {
      list.add(Array(expectedResult, collectionFunctionName, collectionFunction, values))
    }

    def addAny(expected: Any, values: Seq[Any]) = add(expected, "Any", AnyInCollection.apply, values)
    def addAll(expected: Any, values: Seq[Any]) = add(expected, "All", AllInCollection.apply, values)
    def addSingle(expected: Any, values: Seq[Any]) = add(expected, "Single", SingleInCollection.apply, values)
    def addNone(expected: Any, values: Seq[Any]) = add(expected, "None", NoneInCollection.apply, values)

    addAny(expected = false, Seq())
    addAny(expected = true, Seq(true))
    addAny(expected = false, Seq(false))
    addAny(expected = null, Seq(null))
    addAny(expected = true, Seq(null, true))
    addAny(expected = null, Seq(null, false))
    addAny(expected = null, Seq(false, null))
    addAny(expected = true, Seq(true, null))

    addAll(expected = true, Seq())
    addAll(expected = true, Seq(true))
    addAll(expected = false, Seq(false))
    addAll(expected = null, Seq(null))
    addAll(expected = null, Seq(null, true))
    addAll(expected = false, Seq(null, false))
    addAll(expected = false, Seq(false, null))
    addAll(expected = null, Seq(true, null))

    addSingle(expected = false, Seq())
    addSingle(expected = true, Seq(true))
    addSingle(expected = false, Seq(false))
    addSingle(expected = null, Seq(null))
    addSingle(expected = null, Seq(null, true))
    addSingle(expected = null, Seq(null, false))
    addSingle(expected = null, Seq(false, null))
    addSingle(expected = null, Seq(true, null))
    addSingle(expected = true, Seq(true, false))
    addSingle(expected = true, Seq(false, true))
    addSingle(expected = false, Seq(true, true))
    addSingle(expected = false, Seq(false, true, true))
    addSingle(expected = true, Seq(false, true, false))
    addSingle(expected = null, Seq(false, true, null))

    addNone(expected = true, Seq())
    addNone(expected = false, Seq(true))
    addNone(expected = true, Seq(false))
    addNone(expected = null, Seq(null))
    addNone(expected = false, Seq(null, true))
    addNone(expected = null, Seq(null, false))
    addNone(expected = null, Seq(false, null))
    addNone(expected = false, Seq(true, null))
    addNone(expected = false, Seq(true, false))
    addNone(expected = false, Seq(false, true))
    addNone(expected = false, Seq(true, true))
    addNone(expected = false, Seq(false, true, true))
    addNone(expected = false, Seq(false, true, false))
    addNone(expected = false, Seq(false, true, null))

    list
  }

}
