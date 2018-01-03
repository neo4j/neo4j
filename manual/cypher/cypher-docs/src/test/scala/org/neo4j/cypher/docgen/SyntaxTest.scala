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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

class SyntaxTest extends DocumentingTestBase {
  override def graphDescription = List(
    "A:foo:bar KNOWS B",
    "A KNOWS C",
    "B KNOWS D",
    "C KNOWS D",
    "B MARRIED E:Spouse")

  override val properties = Map(
    "A" -> Map[String, Any]("name" -> "Alice", "age" -> 38, "eyes" -> "brown"),
    "B" -> Map[String, Any]("name" -> "Bob", "age" -> 25, "eyes" -> "blue"),
    "C" -> Map[String, Any]("name" -> "Charlie", "age" -> 53, "eyes" -> "green"),
    "D" -> Map[String, Any]("name" -> "Daniel", "age" -> 54, "eyes" -> "brown"),
    "E" -> Map[String, Any]("name" -> "Eskil", "age" -> 41, "eyes" -> "blue", "array" -> Array("one", "two", "three"))
  )

  def section = "syntax"

  val common_arguments = List(
    "collection" -> "An expression that returns a collection",
    "identifier" -> "This is the identifier that can be used from the predicate.",
    "predicate" -> "A predicate that is tested against all items in the collection."
  )



  @Test def simple_case() {
    testThis(
      title = "Simple CASE",
      syntax = """CASE test
WHEN value THEN result
[WHEN ...]
[ELSE default]
END""",

      arguments = List(
        "test" -> "A valid expression.",
        "value" -> "An expression whose result will be compared to the +test+ expression.",
        "result" -> "This is the result expression used if the value expression matches the +test+ expression.",
        "default" -> "The expression to use if no match is found."
      ),
      text = "The expression is calculated, and compared in order with the +WHEN+ clauses until a match is found. " +
        "If no match is found the expression in the +ELSE+ clause is used, or +null+, if no +ELSE+ case exists.",
      queryText =
        """match (n) return CASE n.eyes
    WHEN 'blue'  THEN 1
    WHEN 'brown' THEN 2
                 ELSE 3
END as result""",
      returns = "",
      assertions = (p) => assert(Set(Map("result" -> 2), Map("result" -> 1), Map("result" -> 2), Map("result" -> 1), Map("result" -> 3)) === p.toSet)
    )
  }

  @Test def generic_case() {
    testThis(
      title = "Generic CASE",
      syntax = """CASE
WHEN predicate THEN result
[WHEN ...]
[ELSE default]
END""",

      arguments = List(
        "predicate" -> "A predicate that is tested to find a valid alternative.",
        "result" -> "This is the result expression used if the predicate matches.",
        "default" -> "The expression to use if no match is found."
      ),
      text = "The predicates are evaluated in order until a true value is found, and the result value is used. " +
        "If no match is found the expression in the +ELSE+ clause is used, or +null+, if no +ELSE+ case exists.",
      queryText =
        """match (n) return CASE
    WHEN n.eyes = 'blue'  THEN 1
    WHEN n.age < 40       THEN 2
                          ELSE 3
END as result""",
      returns = "",
      assertions = (p) => assert(Set(Map("result" -> 3), Map("result" -> 1), Map("result" -> 2), Map("result" -> 1), Map("result" -> 3)) === p.toSet)
    )
  }

  private def testThis(title: String, syntax: String, arguments: List[(String, String)], text: String, queryText: String, returns: String, assertions: InternalExecutionResult => Unit) {
    val argsText = arguments.map(x => "* _" + x._1 + ":_ " + x._2).mkString("\r\n\r\n")
    val fullText = String.format("""%s

*Syntax:*
[source,cypher]
----
%s
----

*Arguments:*

%s""", text, syntax, argsText)
    testQuery(title, fullText, queryText, returns, assertions = assertions)
  }
}
