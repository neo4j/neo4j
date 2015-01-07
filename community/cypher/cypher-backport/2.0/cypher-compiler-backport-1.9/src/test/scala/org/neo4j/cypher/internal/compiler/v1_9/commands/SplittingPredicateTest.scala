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
package org.neo4j.cypher.internal.compiler.v1_9.commands

import expressions.Literal
import org.scalatest.Assertions
import org.junit.Test

class SplittingPredicateTest extends Assertions {

  @Test def cantDivideMore() {
    val x = Equals(Literal("a"), Literal("a"))
    assert(x.atoms === Seq(x))
  }

  @Test def andCanBeSplitInTwo() {
    val x = And(True(), True())
    assert(x.atoms === Seq(True(), True()))
  }

  @Test def or_cannot_be_split() {
    val x = Or(True(), True())
    assert(x.atoms === Seq(x))
  }

  @Test def more_complex_splitting() {
    val x = And(
      Equals(
        Literal(1), Literal(2)),
      Or(
        True(), Not(True())
      )
    )

    assert(x.atoms === Seq(Equals(Literal(1), Literal(2)), Or(True(), Not(True()))))
  }
}
