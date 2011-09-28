/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.commands

import org.scalatest.Assertions
import org.neo4j.cypher.SymbolTable
import org.junit.Test

class SplittingClauseTest extends Assertions {

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

class FakeValue(id: Identifier = null, depends: Set[String] = Set()) extends Value {
  def identifier: Identifier = id

  def checkAvailable(symbols: SymbolTable) {}

  def dependsOn: Set[String] = depends

  def apply(v1: Map[String, Any]): Any = null
}