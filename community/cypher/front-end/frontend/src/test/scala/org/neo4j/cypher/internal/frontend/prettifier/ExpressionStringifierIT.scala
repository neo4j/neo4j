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
package org.neo4j.cypher.internal.frontend.prettifier

import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTFactory
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.parser.v5.ast.factory.ast.CypherAstParser
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class ExpressionStringifierIT extends CypherFunSuite {

  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val stringifier: ExpressionStringifier = ExpressionStringifier()

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
      "\"a'\\\"pa\"" -> "\"a'\\\"pa\"",
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
      "not(true)" -> "NOT true",
      "case when 1 = n.prop then 1 when 2 = n.prop then 2 else 4 end" ->
        """CASE
          |  WHEN 1 = n.prop THEN 1
          |  WHEN 2 = n.prop THEN 2
          |  ELSE 4
          |END""".stripMargin,
      """CASE n.name
        |    WHEN IS NULL THEN 1
        |    WHEN IS NOT NORMALIZED THEN 2
        |    WHEN IS NOT NFKD NORMALIZED THEN 3
        |    WHEN IS TYPED BOOLEAN THEN 4
        |    WHEN IS NOT TYPED STRING THEN 5
        |    WHEN :: POINT THEN 6
        |    WHEN STARTS WITH "A" THEN 7
        |    WHEN ENDS WITH "k" THEN 8
        |    WHEN =~ 'C.*t' THEN 9
        |    WHEN IS NOT NULL THEN 10
        |    WHEN IS NORMALIZED THEN 11
        |    ELSE 13
        |END""".stripMargin ->
        """CASE n.name
          |  WHEN IS NULL THEN 1
          |  WHEN IS NOT NFC NORMALIZED THEN 2
          |  WHEN IS NOT NFKD NORMALIZED THEN 3
          |  WHEN IS TYPED BOOLEAN THEN 4
          |  WHEN IS NOT TYPED STRING THEN 5
          |  WHEN IS TYPED POINT THEN 6
          |  WHEN STARTS WITH "A" THEN 7
          |  WHEN ENDS WITH "k" THEN 8
          |  WHEN =~ "C.*t" THEN 9
          |  WHEN IS NOT NULL THEN 10
          |  WHEN IS NFC NORMALIZED THEN 11
          |  ELSE 13
          |END""".stripMargin,
      "case n.prop when 1 then '1' when 2 then '2' else '4' end" ->
        """CASE n.prop
          |  WHEN 1 THEN "1"
          |  WHEN 2 THEN "2"
          |  ELSE "4"
          |END""".stripMargin,
      "case wHen true THEN \"yay\" end" ->
        """CASE
          |  WHEN true THEN "yay"
          |END""".stripMargin,
      "case wHen true THEN \"yay\"   else 1 end" ->
        """CASE
          |  WHEN true THEN "yay"
          |  ELSE 1
          |END""".stripMargin,
      "case c.established wHen < 1500, 1633 THEN \"Old\" wheN > 2000 then \"New\" else \"Middle Aged\"  end" ->
        """CASE c.established
          |  WHEN < 1500 THEN "Old"
          |  WHEN 1633 THEN "Old"
          |  WHEN > 2000 THEN "New"
          |  ELSE "Middle Aged"
          |END""".stripMargin,
      "case c.established wHen 1500 THEN \"Old\" wheN 2000 then \"New\" else \"Middle Aged\"  end" ->
        """CASE c.established
          |  WHEN 1500 THEN "Old"
          |  WHEN 2000 THEN "New"
          |  ELSE "Middle Aged"
          |END""".stripMargin,
      "not(((1) = (2)) and ((3) = (4)))" -> "NOT (1 = 2 AND 3 = 4)",
      "reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age)" ->
        "reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age)",
      "$param1+$param2" -> "$param1 + $param2",
      "(:Label)--()" -> "(:Label)--()",
      "(:Label {prop:1})--()" -> "(:Label {prop: 1})--()",
      "()-[:Type {prop:1}]-()" -> "()-[:Type {prop: 1}]-()",
      "EXISTS { MATCH (n)}" -> "EXISTS { MATCH (n) }",
      "EXISTS { MATCH (n) WHERE n.prop = 'f'}" -> "EXISTS { MATCH (n)\n  WHERE n.prop = \"f\" }",
      "EXISTS { MATCH (n : Label)-[:HAS_REL]->(m) WHERE n.prop = 'f'}" -> "EXISTS { MATCH (n:Label)-[:HAS_REL]->(m)\n  WHERE n.prop = \"f\" }",
      "EXISTS { MATCH (n : Label)-[:HAS_REL]->(m) WHERE n.prop = 'f' RETURN n }" -> "EXISTS { MATCH (n:Label)-[:HAS_REL]->(m)\n  WHERE n.prop = \"f\"\nRETURN n }",
      "COUNT {(n)}" -> "COUNT { MATCH (n) }",
      "COUNT {(n)<-[]->(m)}" -> "COUNT { MATCH (n)--(m) }",
      "COUNT {(n : Label)-[:HAS_REL]->(m) WHERE n.prop = 'f'}" -> "COUNT { MATCH (n:Label)-[:HAS_REL]->(m)\n  WHERE n.prop = \"f\" }",
      "COUNT { MATCH (n : Label)-[:HAS_REL]->(m) WHERE n.prop = 'f' RETURN n }" -> "COUNT { MATCH (n:Label)-[:HAS_REL]->(m)\n  WHERE n.prop = \"f\"\nRETURN n }",
      "reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age) + 4 * 5" -> "reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age) + 4 * 5",
      "1 < 2 > 3 = 4 >= 5 <= 6" -> "1 < 2 > 3 = 4 >= 5 <= 6",
      "1 < 2 > 3 = 4 >= 5 <= 6 AND a OR b" -> "(1 < 2 > 3 = 4 >= 5 <= 6) AND a OR b",
      "x IS TYPED nothing" -> "x IS :: NOTHING",
      "x IS TYPED null" -> "x IS :: NULL",
      "x IS TYPED bool" -> "x IS :: BOOLEAN",
      "n.prop is :: varChar" -> "n.prop IS :: STRING",
      "1 :: InT" -> "1 IS :: INTEGER",
      "['2'] IS not TYPED TIMESTAMP without TIMEZONE" -> "[\"2\"] IS NOT :: LOCAL DATETIME",
      "$param is NOT :: time without TIMEZONE" -> "$param IS NOT :: LOCAL TIME",
      "1 :: SIGNED INTEGER OR 1 IS NOT TYPED point" -> "1 IS :: INTEGER OR 1 IS NOT :: POINT",
      "1 :: ANY VERTEX" -> "1 IS :: NODE",
      "1 :: ANY EDGE" -> "1 IS :: RELATIONSHIP",
      "1 :: ANY MAP" -> "1 IS :: MAP",
      "1 :: path" -> "1 IS :: PATH",
      "1 is typed ANY PROPERTY VALUE" -> "1 IS :: PROPERTY VALUE",
      "1 is typed ANY VALUE" -> "1 IS :: ANY",
      "1 :: LIST < INT   >" -> "1 IS :: LIST<INTEGER>",
      "1 :: ARRAY <  VARcHAr  >" -> "1 IS :: LIST<STRING>",
      "1 :: any value    <  int    | bool   | bool  >" -> "1 IS :: BOOLEAN | INTEGER",
      "1 :: any     <  int    | bool   | bool  >" -> "1 IS :: BOOLEAN | INTEGER",
      "1 ::  int    | bool   | bool  " -> "1 IS :: BOOLEAN | INTEGER",
      "x IS nfc NORMALIZED" -> "x IS NFC NORMALIZED",
      "x IS not normalized" -> "x IS NOT NFC NORMALIZED"
    )

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val expression = parseAntlr(inputString)
        val expressionJavaCc = parseJavaCc(inputString)
        expressionJavaCc shouldBe expression
        stringifier(expression) should equal(expected)
        stringifier(expressionJavaCc) should equal(expected)
      }
  }

  private def parseAntlr(cypher: String): Expression =
    new CypherAstParser(cypher, Neo4jCypherExceptionFactory(cypher, None), None).expression()

  // noinspection TypeAnnotation
  private def parseJavaCc(cypher: String): Expression = {
    val charStream = new CypherCharStream(cypher)
    val astExceptionFactory = new Neo4jASTExceptionFactory(Neo4jCypherExceptionFactory(cypher, None))
    val astFactory = new Neo4jASTFactory(cypher, astExceptionFactory, null)
    new Cypher(astFactory, astExceptionFactory, charStream).Expression()
  }

}
