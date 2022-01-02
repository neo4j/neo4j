/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.symbols

class MultipleGraphClausesParsingTest extends JavaccParserAstTestBase[Clause] {

  implicit private val parser: JavaccRule[Clause] = JavaccRule.Clause

  private val fooBarGraph = expressions.Property(expressions.Variable("foo")(pos), expressions.PropertyKeyName("bar")(pos))(pos)

  val keywords: Seq[(String, expressions.Expression => ast.GraphSelection)] = Seq(
    "USE" -> (ast.UseGraph(_)(pos))
  )

  val graphSelection: Seq[(String, expressions.Expression)] = Seq(
    "GRAPH foo.bar" ->
      fooBarGraph,

    "GRAPH foo()" ->
      expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("foo")(pos), false, IndexedSeq())(pos),

    "GRAPH foo   (    )" ->
      expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("foo")(pos), false, IndexedSeq())(pos),

    "GRAPH foo.bar(baz(grok))" ->
      expressions.FunctionInvocation(expressions.Namespace(List("foo"))(pos), expressions.FunctionName("bar")(pos), false, IndexedSeq(
        expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("baz")(pos), false, IndexedSeq(
          expressions.Variable("grok")(pos)
        ))(pos)
      ))(pos),

    "GRAPH foo. bar   (baz  (grok   )  )" ->
      expressions.FunctionInvocation(expressions.Namespace(List("foo"))(pos), expressions.FunctionName("bar")(pos), false, IndexedSeq(
        expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("baz")(pos), false, IndexedSeq(
          expressions.Variable("grok")(pos)
        ))(pos)
      ))(pos),

    "GRAPH foo.bar(baz(grok), another.name)" ->
      expressions.FunctionInvocation(expressions.Namespace(List("foo"))(pos), expressions.FunctionName("bar")(pos), false, IndexedSeq(
        expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("baz")(pos), false, IndexedSeq(
          expressions.Variable("grok")(pos)
        ))(pos),
        expressions.Property(expressions.Variable("another")(pos), expressions.PropertyKeyName("name")(pos))(pos)
      ))(pos),

    "foo.bar(baz(grok), another.name)" ->
      expressions.FunctionInvocation(expressions.Namespace(List("foo"))(pos), expressions.FunctionName("bar")(pos), false, IndexedSeq(
        expressions.FunctionInvocation(expressions.Namespace()(pos), expressions.FunctionName("baz")(pos), false, IndexedSeq(
          expressions.Variable("grok")(pos)
        ))(pos),
        expressions.Property(expressions.Variable("another")(pos), expressions.PropertyKeyName("name")(pos))(pos)
      ))(pos),

    "foo.bar(1, $par)" ->
      expressions.FunctionInvocation(expressions.Namespace(List("foo"))(pos), expressions.FunctionName("bar")(pos), false, IndexedSeq(
        expressions.SignedDecimalIntegerLiteral("1")(pos),
        expressions.Parameter("par", symbols.CTAny)(pos)
      ))(pos),

    "a + b" ->
      expressions.Add(expressions.Variable("a")(pos), expressions.Variable("b")(pos))(pos),

    "GRAPH graph" ->
      expressions.Variable("graph")(pos),

    "`graph`" ->
      expressions.Variable("graph")(pos),

    "graph1" ->
      expressions.Variable("graph1")(pos),

    "`foo.bar.baz.baz`" ->
      expressions.Variable("foo.bar.baz.baz")(pos),

    "GRAPH `foo.bar`.baz" ->
      expressions.Property(expressions.Variable("foo.bar")(pos), expressions.PropertyKeyName("baz")(pos))(pos),

    "GRAPH foo.`bar.baz`" ->
      expressions.Property(expressions.Variable("foo")(pos), expressions.PropertyKeyName("bar.baz")(pos))(pos),

    "GRAPH `foo.bar`.`baz.baz`" ->
      expressions.Property(expressions.Variable("foo.bar")(pos), expressions.PropertyKeyName("baz.baz")(pos))(pos),
  )

  for {
    (keyword, clause) <- keywords
    (input, expectedExpression) <- graphSelection
  } {
    test(s"$keyword $input") {
      gives(clause(expectedExpression))
    }
  }
}
