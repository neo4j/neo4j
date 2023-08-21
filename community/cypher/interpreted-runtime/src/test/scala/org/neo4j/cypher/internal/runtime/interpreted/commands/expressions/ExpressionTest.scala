/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Not
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue

import scala.collection.Map

class ExpressionTest extends CypherFunSuite {

  test("replacePropWithCache") {
    val a = Collect(Property(Variable("r"), PropertyKey("age")))

    val b = a.rewrite {
      case Property(n, p) => literal(n + "." + p.name)
      case x              => x
    }

    b should equal(Collect(literal("r.age")))
  }

  test("merge_two_different_variables") {
    testMerge(
      Map("a" -> CTAny),
      Map("b" -> CTAny),
      Map("a" -> CTAny, "b" -> CTAny)
    )
  }

  test("merge_two_deps_on_the_same_variable") {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTAny),
      Map("a" -> CTAny)
    )
  }

  test("merge_two_deps_same_id_different_types") {
    testMerge(
      Map("a" -> CTAny),
      Map("a" -> CTMap),
      Map("a" -> CTAny)
    )
  }

  test("should_find_inner_aggregations") {
    // GIVEN
    val e = LengthFunction(Collect(Property(Variable("n"), PropertyKey("bar"))))

    // WHEN
    val hasAggregates = e.exists(e => e.isInstanceOf[AggregationExpression])

    // THEN
    hasAggregates shouldBe true
  }

  test("should_handle_rewriting_to_non_predicates") {
    // given
    val expression = Not(True())

    // when
    val result = expression.rewrite {
      case True() => literal(true)
      case e      => e
    }

    // then
    result should equal(Not(CoercedPredicate(literal(true))))
  }

  private def testMerge(
    a: Map[String, CypherType],
    b: Map[String, CypherType],
    expected: Map[String, CypherType]
  ): Unit = {
    merge(a, b, expected)
    merge(b, a, expected)
  }

  val e = new TestExpression

  private def merge(a: Map[String, CypherType], b: Map[String, CypherType], expected: Map[String, CypherType]): Unit = {

    val keys = (a.keys ++ b.keys).toSet

    if (keys != expected.keys.toSet) {
      fail("Wrong keys found: " + keys + " vs. " + expected.keys.toSet)
    }

    val result = keys.toSeq.map(k =>
      (a.get(k), b.get(k)) match {
        case (Some(x), None)    => k -> x
        case (None, Some(x))    => k -> x
        case (Some(x), Some(y)) => k -> x.leastUpperBound(y)
        case (None, None)       => throw new AssertionError("only here to stop warnings")
      }
    ).toMap

    if (result != expected) {
      fail(s"""
Merged:
    $a with
    $b

     Got: $result
Expected: $expected""")
    }
  }
}

class TestExpression extends Expression {
  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = null

  override def apply(row: ReadableRow, state: QueryState): AnyValue = null
}
