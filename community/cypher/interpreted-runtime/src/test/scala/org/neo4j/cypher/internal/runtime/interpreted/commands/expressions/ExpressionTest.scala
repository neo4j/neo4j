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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.ReturnItem
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{CoercedPredicate, Not, True}
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

import scala.collection.Map

class ExpressionTest extends CypherFunSuite {
  test("replacePropWithCache") {
    val a = Collect(Property(Variable("r"), PropertyKey("age")))

    val b = a.rewrite {
      case Property(n, p) => Literal(n + "." + p.name)
      case x              => x
    }

    b should equal(Collect(Literal("r.age")))
  }

  test("merge_two_different_variables") {
    testMerge(
      Map("a" -> CTAny),
      Map("b" -> CTAny),
      Map("a" -> CTAny, "b" -> CTAny))
  }

  test("merge_two_deps_on_the_same_variable") {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTAny),
      Map("a" -> CTAny))
  }

  test("merge_two_deps_same_id_different_types") {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTMap),
      Map("a" -> CTAny))
  }

  test("should_find_inner_aggregations") {
    //GIVEN
    val e = LengthFunction(Collect(Property(Variable("n"), PropertyKey("bar"))))

    //WHEN
    val aggregates = e.filter(e => e.isInstanceOf[AggregationExpression])

    //THEN
    aggregates.toList should equal( List(Collect(Property(Variable("n"), PropertyKey("bar")))))
  }

  test("should_find_inner_aggregations2") {
    //GIVEN
    val r = ReturnItem(Avg(Property(Variable("a"), PropertyKey("age"))), "avg(a.age)")

    //WHEN
    val aggregates = r.expression.filter(e => e.isInstanceOf[AggregationExpression])

    //THEN
    aggregates.toList should equal( List(Avg(Property(Variable("a"), PropertyKey("age")))))
  }

  test("should_handle_rewriting_to_non_predicates") {
    // given
    val expression = Not(True())

    // when
    val result = expression.rewrite {
      case True() => Literal(true)
      case e      => e
    }

    // then
    result should equal(Not(CoercedPredicate(Literal(true))))
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
      case (Some(x), Some(y)) => k -> x.leastUpperBound(y)
      case (None, None)       => throw new AssertionError("only here to stop warnings")
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

  def symbolTableDependencies = Set()

  def apply(v1: ExecutionContext, state: QueryState) = null
}
