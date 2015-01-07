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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.values.TokenType._
import commands.{CoercedPredicate, True, Not, ReturnItem}
import pipes.QueryState
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.junit.Test
import org.scalatest.Assertions
import collection.Map

class ExpressionTest extends Assertions {
  @Test def replacePropWithCache() {
    val a = Collect(Property(Identifier("r"), PropertyKey("age")))

    val b = a.rewrite {
      case Property(n, p) => Literal(n + "." + p.name)
      case x              => x
    }

    assert(b === Collect(Literal("r.age")))
  }

  @Test def merge_two_different_identifiers() {
    testMerge(
      Map("a" -> CTAny),
      Map("b" -> CTAny),

      Map("a" -> CTAny, "b" -> CTAny))
  }

  @Test def merge_two_deps_on_the_same_identifier() {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTAny),

      Map("a" -> CTAny))
  }

  @Test def merge_two_deps_same_id_different_types() {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTMap),

      Map("a" -> CTAny))
  }

  @Test
  def should_find_inner_aggregations() {
    //GIVEN
    val e = LengthFunction(Collect(Property(Identifier("n"), PropertyKey("bar"))))

    //WHEN
    val aggregates = e.filter(e => e.isInstanceOf[AggregationExpression])

    //THEN
    assert(aggregates.toList ===  List(Collect(Property(Identifier("n"), PropertyKey("bar")))))
  }

  @Test
  def should_find_inner_aggregations2() {
    //GIVEN
    val r = ReturnItem(Avg(Property(Identifier("a"), PropertyKey("age"))), "avg(a.age)")

    //WHEN
    val aggregates = r.expression.filter(e => e.isInstanceOf[AggregationExpression])

    //THEN
    assert(aggregates.toList ===  List(Avg(Property(Identifier("a"), PropertyKey("age")))))
  }

  @Test
  def should_handle_rewriting_to_non_predicates() {
    // given
    val expression = Not(True())

    // when
    val result = expression.rewrite {
      case True() => Literal(true)
      case e      => e
    }

    // then
    assert(result === Not(CoercedPredicate(Literal(true))))
  }

  private def testMerge(a: Map[String, CypherType], b: Map[String, CypherType], expected: Map[String, CypherType]) {
    merge(a, b, expected)
    merge(b, a, expected)
  }

  val e = new TestExpression

  private def merge(a: Map[String, CypherType], b: Map[String, CypherType], expected: Map[String, CypherType]) {

    val keys = (a.keys ++ b.keys).toSet

    if (keys != expected.keys.toSet) {
      fail("Wrong keys found: " + keys + " vs. " + expected.keys.toSet)
    }

    val result = keys.toSeq.map(k => (a.get(k), b.get(k)) match {
      case (Some(x), None)    => k -> x
      case (None, Some(x))    => k -> x
      case (Some(x), Some(y)) => k -> x.mergeUp(y)
      case (None, None)       => throw new ThisShouldNotHappenError("Andres", "only here to stop warnings")
    }).toMap

    if (result != expected) {
      fail("""
Merged:
    %s with
    %s

     Got: %s
Expected: %s""".format(a, b, result, expected))
    }
  }
}

class TestExpression extends Expression {
  def arguments = Nil

  def rewrite(f: (Expression) => Expression): Expression = null

  def calculateType(symbols: SymbolTable): CypherType = null

  def symbolTableDependencies = Set()

  def apply(v1: ExecutionContext)(implicit state: QueryState) = null
}
