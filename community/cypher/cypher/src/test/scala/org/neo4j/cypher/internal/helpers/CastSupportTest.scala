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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.CypherTypeException
import org.scalatest.Assertions
import org.junit.Test

class CastSupportTest extends Assertions {

  @Test def siftTest() {
    val given = Seq(1, 2, "a", 3, "b", 42, "z")
    val then  = CastSupport.sift[String](given)
    assert(then == Seq("a", "b", "z"))
  }

  @Test def siftComplexTest() {
    val given = Seq(1, 2, List("a"), 3, "b", 42, List("z"))
    val then  = CastSupport.sift[List[String]](given)
    assert(then == Seq(List("a"), List("z")))
  }

  @Test def downcastPfMatchTest() {
    val given: Any                          = Seq(1)
    val fun: PartialFunction[Any, Seq[Int]] = CastSupport.erasureCast[Seq[Int]]
    val then                                = fun(given)
    assert(then == Seq(1))
  }

  @Test def downcastPfMismatchTest() {
    val given: Any                           = "Hallo"
    val fun: PartialFunction[Any, Seq[Long]] = CastSupport.erasureCast[Seq[Long]]
    assert(! fun.isDefinedAt(given))
  }

  @Test def downcastAppMatchTest() {
    val given: Any = 1
    assert(CastSupport.castOrFail[java.lang.Integer](given) == 1)
  }

  @Test def downcastAppMismatchTest() {
    val given: Any = Seq(1)
    intercept[CypherTypeException](CastSupport.castOrFail[Int](given))
  }
}
