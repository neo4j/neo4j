/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.opencypher.v9_0.util.{DummyPosition, Rewriter}
import org.opencypher.v9_0.util.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.opencypher.v9_0.expressions._

trait PredicateTestSupport extends CypherTestSupport {
  self: CypherFunSuite =>

  private val pos = DummyPosition(0)

  def rewriter: Rewriter

  val P = anExp("P")
  val Q = anExp("Q")
  val R = anExp("R")
  val S = anExp("S")
  val V = anExp("V")

  implicit class IFF(x: Expression) {
    def <=>(other: Expression) = {
      val output = rewriter(x)

      output should equal(other)
    }
  }

  def anExp(s: String) = StringLiteral(s)(pos)
  def and(p1: Expression, p2: Expression) = And(p1, p2)(pos)
  def ands(predicates: Expression*) = Ands(predicates.toSet)(pos)
  def or(p1: Expression, p2: Expression) = Or(p1, p2)(pos)
  def ors(predicates: Expression*) = Ors(predicates.toSet)(pos)
  def xor(p1: Expression, p2: Expression) = Xor(p1, p2)(pos)
  def not(e: Expression) = Not(e)(pos)
  def TRUE: Expression = True()(pos)
  def FALSE: Expression = False()(pos)
}
