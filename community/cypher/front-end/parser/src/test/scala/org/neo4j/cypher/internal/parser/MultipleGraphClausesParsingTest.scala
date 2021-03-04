/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.symbols
import org.parboiled.scala.Rule1

class MultipleGraphClausesParsingTest
  extends ParserAstTest[ast.Clause]
    with Query
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Clause] = Clause

  val fooBarGraph = expressions.Property(expressions.Variable("foo")(pos), expressions.PropertyKeyName("bar")(pos))(pos)

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
