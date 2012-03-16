/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import org.scalatest.Assertions
import collection.Map
import org.junit.{Assert, Test}
import org.neo4j.cypher.internal.symbols.{Identifier, AnyType}


class CoalesceTest extends Assertions {
  @Test def givenANonNullValueThenReturnsTheValue() {
    val func = new CoalesceFunction(Literal("a"))
    assert( func(Map()) === "a" )
  }

  @Test def givenANullValueThenReturnsNull() {
    val func = new CoalesceFunction(Null())
    assert( func(Map()) === null )
  }

  @Test def givenOneNullAndOneValueThenReturnsTheValue() {
    val func = new CoalesceFunction(Null(), Literal("Alistair"))
    assert( func(Map()) === "Alistair" )
  }

  @Test def coalesce_should_be_lazy() {
    val func = new CoalesceFunction(Literal("Hunger"), BreakingExpression())
    assert( func(Map()) === "Hunger" )
  }
}

case class BreakingExpression() extends Expression {
  protected def compute(v1: Map[String, Any]) = Assert.fail("Coalesce is not lazy")

  val identifier = Identifier("breaking",AnyType())

  def declareDependencies(extectedType: AnyType) = null

  def rewrite(f: (Expression) => Expression) = null

  def filter(f: (Expression) => Boolean) = null
}
