/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

trait PredicateTestSupport {
  self: CypherFunSuite =>

  private val pos = DummyPosition(0)

  def rewriter: Rewriter

  val P: Expression = anExp("P")
  val Q: Expression = anExp("Q")
  val R: Expression = anExp("R")
  val S: Expression = anExp("S")
  val T: Expression = anExp("T")
  val V: Expression = anExp("V")

  implicit class IFF(x: Expression) {

    def <=>(other: Expression): Assertion = {
      val output = rewriter(x)

      output should equal(other)
    }
  }

  def anExp(s: String): Expression = StringLiteral(s)(pos.withInputLength(0))
  def and(p1: Expression, p2: Expression): Expression = And(p1, p2)(pos)
  def ands(predicates: Expression*): Expression = Ands(predicates)(pos)
  def or(p1: Expression, p2: Expression): Or = Or(p1, p2)(pos)
  def ors(predicates: Expression*): Expression = Ors(predicates)(pos)
  def xor(p1: Expression, p2: Expression): Expression = Xor(p1, p2)(pos)
  def not(e: Expression): Expression = Not(e)(pos)
  def bool(e: Expression): Expression = CoerceToPredicate(e)
  def TRUE: Expression = True()(pos)
  def FALSE: Expression = False()(pos)
}
