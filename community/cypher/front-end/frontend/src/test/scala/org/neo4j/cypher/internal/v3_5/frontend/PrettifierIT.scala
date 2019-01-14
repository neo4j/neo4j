/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.frontend

import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v3_5.parser.CypherParser
import org.neo4j.cypher.internal.v3_5.util.test_helpers.{CypherFunSuite, WindowsStringSafe}

class PrettifierIT extends CypherFunSuite {
  implicit val windowsSafe = WindowsStringSafe

  val stringifier: Prettifier = Prettifier(ExpressionStringifier())

  val parser = new CypherParser
  val tests: Seq[(String, String)] =
    Seq[(String, String)](
      "return 42" -> "RETURN 42",
      "return 42 as x" -> "RETURN 42 AS x",
      "return 42 as `43`" -> "RETURN 42 AS `43`",
      "return distinct 42" -> "RETURN DISTINCT 42",

      "return distinct a, b as X, 3+3 as six order by b.prop, b.foo descending skip 1 limit 2" ->
        """RETURN DISTINCT a, b AS X, 3 + 3 AS six
          |  ORDER BY b.prop ASCENDING, b.foo DESCENDING
          |  SKIP 1
          |  LIMIT 2""".stripMargin,

      "match (a) return a" ->
        """MATCH (a)
          |RETURN a""".stripMargin,

      "match (a) where a.prop = 42 return a" ->
        """MATCH (a)
          |  WHERE a.prop = 42
          |RETURN a""".stripMargin,

      "match (a) with distinct a, b as X, 3+3 as six order by b.prop, b.foo descending skip 1 limit 2 where true" ->
        """MATCH (a)
          |WITH DISTINCT a, b AS X, 3 + 3 AS six
          |  ORDER BY b.prop ASCENDING, b.foo DESCENDING
          |  SKIP 1
          |  LIMIT 2
          |  WHERE true""".stripMargin,

      "create (a)--(b) RETURN a" ->
        """CREATE (a)--(b)
          |RETURN a""".stripMargin,

      "match (a:Label {prop: 1}) RETURN a" ->
        """MATCH (a:Label {prop: 1})
          |RETURN a""".stripMargin,

      "unwind [1,2,3] AS x RETURN x" ->
        """UNWIND [1, 2, 3] AS x
          |RETURN x""".stripMargin,

      "CALL nsp.proc()" ->
        """CALL nsp.proc()""".stripMargin,

      "CALL proc()" ->
        """CALL proc()""".stripMargin,

      "CALL nsp1.nsp2.proc()" ->
        """CALL nsp1.nsp2.proc()""".stripMargin,

      "CALL nsp.proc(a)" ->
        """CALL nsp.proc(a)""".stripMargin,

      "CALL nsp.proc(a,b)" ->
        """CALL nsp.proc(a, b)""".stripMargin,

      "CALL nsp.proc() yield x" ->
        """CALL nsp.proc() YIELD x""".stripMargin,

      "CALL nsp.proc() yield x, y" ->
        """CALL nsp.proc() YIELD x, y""".stripMargin,

      "match (n) SET n.prop = 1" ->
        """MATCH (n)
          |SET n.prop = 1""".stripMargin,

      "match (n) SET n.prop = 1, n.prop2 = 2" ->
        """MATCH (n)
          |SET n.prop = 1, n.prop2 = 2""".stripMargin,

      "match (n) SET n:Label" ->
        """MATCH (n)
          |SET n:Label""".stripMargin,

      "match (n) SET n:`La bel`" ->
        """MATCH (n)
          |SET n:`La bel`""".stripMargin,

      "match (n) SET n:Label:Bla" ->
        """MATCH (n)
          |SET n:Label:Bla""".stripMargin,

      "match (n) SET n += {prop: 1}" ->
        """MATCH (n)
          |SET n += {prop: 1}""".stripMargin,

      "match (n) SET n = {prop: 1}" ->
        """MATCH (n)
          |SET n = {prop: 1}""".stripMargin,

      "match (n) SET n:Label, n.prop = 1" ->
        """MATCH (n)
          |SET n:Label, n.prop = 1""".stripMargin,

      "match (n) DELETE n" ->
        """MATCH (n)
          |DELETE n""".stripMargin,

      "match (n), (m) DELETE n, m" ->
        """MATCH (n), (m)
          |DELETE n, m""".stripMargin,

      "merge (n)" ->
        "MERGE (n)",

      "merge (n)--(m)" ->
        "MERGE (n)--(m)",

      "merge (n:Label {prop:1})--(m)" ->
        "MERGE (n:Label {prop: 1})--(m)"
    )

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults: Statement = parser.parse(inputString)
        val str = stringifier.asString(parsingResults)
        str should equal(expected)
      }
  }

}
