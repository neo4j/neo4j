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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.symbols

class MultipleGraphClausesParsingTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  val keywords: Seq[(String, GraphReference => ast.UseGraph)] = Seq(
    "USE" -> use,
    "USE GRAPH" -> use
  )

  val graphSelection: Seq[(String, GraphReference)] = Seq(
    "foo.bar" ->
      GraphDirectReference(CatalogName(List.apply("foo", "bar")))(pos),
    "(foo.bar)" ->
      GraphDirectReference(CatalogName(List.apply("foo", "bar")))(pos),
    "((foo.bar))" ->
      GraphDirectReference(CatalogName(List.apply("foo", "bar")))(pos),
    "foo()" ->
      GraphFunctionReference(function("foo"))(pos),
    "foo   (    )" ->
      GraphFunctionReference(function("foo"))(pos),
    "graph.foo" ->
      GraphDirectReference(CatalogName(List("graph", "foo")))(pos),
    "graph.foo()" ->
      GraphFunctionReference(function("graph", "foo")())(pos),
    "foo.bar(baz(grok))" ->
      GraphFunctionReference(function("foo", "bar")(function("baz")(varFor("grok"))))(pos),
    "foo. bar   (baz  (grok   )  )" ->
      GraphFunctionReference(function("foo", "bar")(function("baz")(varFor("grok"))))(pos),
    "foo.bar(baz(grok), another.name)" ->
      GraphFunctionReference(function("foo", "bar")(function("baz")(varFor("grok")), prop(varFor("another"), "name")))(
        pos
      ),
    "foo.bar(1, $par)" ->
      GraphFunctionReference(
        function("foo", "bar")(
          literalInt(1),
          parameter("par", symbols.CTAny)
        )
      )(pos),
    "`graph`" ->
      GraphDirectReference(CatalogName(List("graph")))(pos),
    "graph1" ->
      GraphDirectReference(CatalogName(List("graph1")))(pos),
    "`foo.bar.baz.baz`" ->
      GraphDirectReference(CatalogName(List("foo.bar.baz.baz")))(pos),
    "`foo.bar`.baz" ->
      GraphDirectReference(CatalogName(List("foo.bar", "baz")))(pos),
    "foo.`bar.baz`" ->
      GraphDirectReference(CatalogName(List("foo", "bar.baz")))(pos),
    "`foo.bar`.`baz.baz`" ->
      GraphDirectReference(CatalogName(List("foo.bar", "baz.baz")))(pos)
  )

  val fullGraphSelections: Seq[(String, ast.GraphSelection)] = Seq(
    "USE GRAPH graph()" -> use(function("graph")()),
    // Interpreted as GRAPH keyword, followed by parenthesized expression
    "USE graph(x)" -> use(List.apply("x"))
  )

  val combinations: Seq[(String, GraphSelection)] = for {
    (keyword, clause) <- keywords
    (input, expectedGraphReference) <- graphSelection
  } yield s"$keyword $input" -> clause(expectedGraphReference)

  for {
    (input, expected) <- combinations ++ fullGraphSelections
  } {
    test(input) {
      gives[Clause](expected)
    }
  }

  private def function(nameParts: String*)(args: expressions.Expression*) =
    expressions.FunctionInvocation(
      expressions.Namespace(nameParts.init.toList)(pos),
      expressions.FunctionName(nameParts.last)(pos),
      distinct = false,
      args.toIndexedSeq
    )(pos)

}
