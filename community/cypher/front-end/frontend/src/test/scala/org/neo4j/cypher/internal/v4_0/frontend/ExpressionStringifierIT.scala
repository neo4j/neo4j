/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.parser.Expressions
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.parboiled.scala.{Parser, ReportingParseRunner}

class ExpressionStringifierIT extends CypherFunSuite with Parser with Expressions {
  val stringifier = ExpressionStringifier()
  val parser = ReportingParseRunner(Expression)

  val tests: Seq[(String, String)] =
    Seq[(String, String)](
      "42" -> "42",
      "[1,2,3,4]" -> "[1, 2, 3, 4]",
      "1+2" -> "1 + 2",
      "(1)+2" -> "1 + 2",
      "(1+2)*3" -> "(1 + 2) * 3",
      "1+2*3" -> "1 + 2 * 3",
      "collect(n)[3]" -> "collect(n)[3]",
      "collect(n)[3..4]" -> "collect(n)[3..4]",
      "collect(n)[2..]" -> "collect(n)[2..]",
      "collect(n)[..2]" -> "collect(n)[..2]",
      "[x in [1,2,3] | x * 2]" -> "[x IN [1, 2, 3] | x * 2]",
      "[x in [1,2,3]\n\tWHERE x%2=0|x*2]" -> "[x IN [1, 2, 3] WHERE x % 2 = 0 | x * 2]",
      "[x in [1,2,3]\n\tWHERE x%2=0]" -> "[x IN [1, 2, 3] WHERE x % 2 = 0]",
      "[(a)-->(b)|a.prop]" -> "[(a)-->(b) | a.prop]",
      "[p=(a)-->(b) WHERE a:APA|a.prop*length(p)]" -> "[p = (a)-->(b) WHERE a:APA | a.prop * length(p)]",
      "n['apa']" -> "n[\"apa\"]",
      "'apa'" -> "\"apa\"",
      "'a\"pa'" -> "'a\"pa'",
      "\"a'pa\"" -> "\"a'pa\"",
      "\"a'\\\"pa\"" ->  "\"a'\\\"pa\"",
      "any(x in ['a','b', 'c'] where x > 28)" -> "any(x IN [\"a\", \"b\", \"c\"] WHERE x > 28)",
      "all(x in ['a','b', 'c'] where x > 28)" -> "all(x IN [\"a\", \"b\", \"c\"] WHERE x > 28)",
      "none(x in ['a','b', 'c'] where x > 28)" -> "none(x IN [\"a\", \"b\", \"c\"] WHERE x > 28)",
      "{k: 'apa', id: 42}" -> "{k: \"apa\", id: 42}",
      "()<--()-->()" -> "()<--()-->()",
      "()<-[*]-()" -> "()<-[*]-()",
      "()<-[*1..]-()" -> "()<-[*1..]-()",
      "()<-[*..2]-()" -> "()<-[*..2]-()",
      "()<-[*2..4]-()" -> "()<-[*2..4]-()",
      "(:Label)<-[var]-({id:43})-->(v:X)" -> "(:Label)<-[var]-({id: 43})-->(v:X)",
      "n{.*,.bar,baz:42,variable}" -> "n{.*, .bar, baz: 42, variable}",
      "n:A:B" -> "n:A:B",
      "not(true)" -> "not true",
      "case when 1 = n.prop then 1 when 2 = n.prop then 2 else 4 end" ->
        "CASE WHEN 1 = n.prop THEN 1 WHEN 2 = n.prop THEN 2 ELSE 4 END",
      "case n.prop when 1 then '1' when 2 then '2' else '4' end" ->
        "CASE n.prop WHEN 1 THEN \"1\" WHEN 2 THEN \"2\" ELSE \"4\" END",
      "not(((1) = (2)) and ((3) = (4)))" -> "not (1 = 2 AND 3 = 4)",
      "reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age)" ->
        "reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age)",
      "$param1+$param2" -> "$param1 + $param2",
      "(:Label)--()" -> "(:Label)--()",
      "(:Label {prop:1})--()" -> "(:Label {prop: 1})--()",
      "()-[:Type {prop:1}]-()" -> "()-[:Type {prop: 1}]-()",
      "EXISTS { MATCH (n)}" -> "EXISTS { MATCH (n) }",
      "EXISTS { MATCH (n) WHERE n.prop = 'f'}" -> "EXISTS { MATCH (n) WHERE n.prop = \"f\" }",
      "EXISTS { MATCH (n : Label)-[:HAS_REL]->(m) WHERE n.prop = 'f'}" -> "EXISTS { MATCH (n:Label)-[:HAS_REL]->(m) WHERE n.prop = \"f\" }",
      "reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age) + 4 * 5" -> "reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age) + 4 * 5"
    )

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults = parser.run(inputString)
        if (parsingResults.parseErrors.nonEmpty) {
          fail("Parsing failed")
        }
        val value1: Expression = parsingResults.result.get

        val str = stringifier(value1)
        str should equal(expected)
      }
  }

}
