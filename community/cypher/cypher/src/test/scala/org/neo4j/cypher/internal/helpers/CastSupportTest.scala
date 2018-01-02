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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.internal.compiler.v2_3.helpers
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CastSupportTest extends CypherFunSuite {

  test("siftTest") {
    val given = Seq[Any](1, 2, "a", 3, "b", 42, "z")
    val then  = helpers.CastSupport.sift[String](given)
    then should equal(Seq("a", "b", "z"))
  }

  test("siftComplexTest") {
    val given = Seq[Any](1, 2, List("a"), 3, "b", 42, List("z"))
    val then  = helpers.CastSupport.sift[List[String]](given)
    then should equal(Seq(List("a"), List("z")))
  }

  test("downcastPfMatchTest") {
    val given: Any                          = Seq(1)
    val fun: PartialFunction[Any, Seq[Int]] = helpers.CastSupport.erasureCast[Seq[Int]]
    val then                                = fun(given)
    then should equal(Seq(1))
  }

  test("downcastPfMismatchTest") {
    val given: Any                           = "Hallo"
    val fun: PartialFunction[Any, Seq[Long]] = helpers.CastSupport.erasureCast[Seq[Long]]
    fun.isDefinedAt(given) should equal(false)
  }

  test("downcastAppMatchTest") {
    val given: Any = 1
    helpers.CastSupport.castOrFail[java.lang.Integer](given) should equal(1)
  }

  test("downcastAppMismatchTest") {
    val given: Any = Seq(1)
    intercept[CypherTypeException](helpers.CastSupport.castOrFail[Int](given))
  }
}
