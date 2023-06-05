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
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.util.symbols

class MultipleGraphClausesParsingTest extends ParserSyntaxTreeBase[Cst.Clause, Clause] {

  implicit private val javaccRule = JavaccRule.Clause
  implicit private val antlrRule = AntlrRule.Clause

  val keywords: Seq[(String, expressions.Expression => ast.GraphSelection)] = Seq(
    "USE" -> use,
    "USE GRAPH" -> use
  )

  val graphSelection: Seq[(String, expressions.Expression)] = Seq(
    "foo.bar" ->
      prop(varFor("foo"), "bar"),
    "(foo.bar)" ->
      prop(varFor("foo"), "bar"),
    "foo()" ->
      function("foo")(),
    "foo   (    )" ->
      function("foo")(),
    "graph.foo" ->
      prop(varFor("graph"), "foo"),
    "graph.foo()" ->
      function("graph", "foo")(),
    "foo.bar(baz(grok))" ->
      function("foo", "bar")(function("baz")(varFor("grok"))),
    "foo. bar   (baz  (grok   )  )" ->
      function("foo", "bar")(function("baz")(varFor("grok"))),
    "foo.bar(baz(grok), another.name)" ->
      function("foo", "bar")(function("baz")(varFor("grok")), prop(varFor("another"), "name")),
    "foo.bar(1, $par)" ->
      function("foo", "bar")(
        literalInt(1),
        parameter("par", symbols.CTAny)
      ),
    "a + b" ->
      add(varFor("a"), varFor("b")),
    "`graph`" ->
      varFor("graph"),
    "graph1" ->
      varFor("graph1"),
    "`foo.bar.baz.baz`" ->
      varFor("foo.bar.baz.baz"),
    "`foo.bar`.baz" ->
      prop(varFor("foo.bar"), "baz"),
    "foo.`bar.baz`" ->
      prop(varFor("foo"), "bar.baz"),
    "`foo.bar`.`baz.baz`" ->
      prop(varFor("foo.bar"), "baz.baz")
  )

  val fullGraphSelections: Seq[(String, ast.GraphSelection)] = Seq(
    "USE GRAPH graph()" -> use(function("graph")()),
    // Interpreted as GRAPH keyword, followed by parenthesized expression
    "USE graph(x)" -> use(varFor("x"))
  )

  val combinations: Seq[(String, GraphSelection)] = for {
    (keyword, clause) <- keywords
    (input, expectedExpression) <- graphSelection
  } yield s"$keyword $input" -> clause(expectedExpression)

  for {
    (input, expected) <- combinations ++ fullGraphSelections
  } {
    test(input) {
      gives(expected)
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
