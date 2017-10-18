/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.parser.Expressions
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.parboiled.scala.{Parser, ReportingParseRunner, _}

class ExpressionStringifierTest
  extends CypherFunSuite
    with AstConstructionTestSupport with Parser with Expressions {
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
      "filter(x in [1,2,3] WHERE x is not null)" -> "filter(x IN [1, 2, 3] WHERE x IS NOT NULL)",
      "collect(n)[3]" -> "collect(n)[3]",
      "collect(n)[3..4]" -> "collect(n)[3..4]",
      "collect(n)[2..]" -> "collect(n)[2..]",
      "collect(n)[..2]" -> "collect(n)[..2]",
      "[x in [1,2,3] | x * 2]" -> "[x IN [1, 2, 3] | x * 2]",
      "[x in [1,2,3]\n\tWHERE x%2=0|x*2]" -> "[x IN [1, 2, 3] WHERE x % 2 = 0 | x * 2]",
      "[x in [1,2,3]\n\tWHERE x%2=0]" -> "[x IN [1, 2, 3] WHERE x % 2 = 0]",
      "[(a)-->(b)|a.prop]" -> "[(a)-->(b) | a.prop]",
      "[p=(a)-->(b) WHERE a:APA|a.prop*size(p)]" -> "[p = (a)-->(b) WHERE a:APA | a.prop * size(p)]",
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
      "extract(x in [1,2,3] | x * 2)" -> "extract(x IN [1, 2, 3] | x * 2)",
      "case when 1 = n.prop then 1 when 2 = n.prop then 2 else 4 end" ->
        "case when 1 = n.prop then 1 when 2 = n.prop then 2 else 4 end",
      "case n.prop when 1 then '1' when 2 then '2' else '4' end" ->
        "case n.prop when 1 then \"1\" when 2 then \"2\" else \"4\" end",
      "not(((1) = (2)) and ((3) = (4)))" -> "not (1 = 2 AND 3 = 4)",
      "reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age)" ->
        "reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age)"
    )

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults = parser.run(inputString)
        val value1: Expression = parsingResults.result.get

        val str = stringifier(value1)
        str should equal(expected)
      }
  }

}
