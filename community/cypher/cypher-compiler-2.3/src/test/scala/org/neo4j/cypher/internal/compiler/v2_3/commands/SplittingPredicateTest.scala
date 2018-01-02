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

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SplittingPredicateTest extends CypherFunSuite {

  test("cantDivideMore") {
    val x = Equals(Literal("a"), Literal("a"))
    x.atoms should equal(Seq(x))
  }

  test("andCanBeSplitInTwo") {
    val x = And(Not(Not(True())), Not(True()))
    x.atoms should equal(Seq(Not(Not(True())), Not(True())))
  }

  test("or_cannot_be_split") {
    val x = Or(True(), True())
    x.atoms should equal(Seq(x))
  }

  test("more_complex_splitting") {
    val x = And(
      Equals(
        Literal(1), Literal(2)),
      Or(
        True(), Not(True())
      )
    )

    x.atoms should equal(Seq(Equals(Literal(1), Literal(2)), Or(True(), Not(True()))))
  }
}
