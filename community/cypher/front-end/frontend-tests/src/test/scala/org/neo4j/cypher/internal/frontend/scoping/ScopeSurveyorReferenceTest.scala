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
package org.neo4j.cypher.internal.frontend.scoping

class ScopeSurveyorReferenceTest extends VariableCheckingTestSuite {

  test("""WITH 1 AS x
         |RETURN x""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(
        varOf("x", 10),
        Seq(varOf("x", 10), varOf("x", 19), varOf("x", 19)),
        None
      )
    ))
  }

  test("""WITH 1 AS x
         |CALL () {
         |  WITH 2 AS x
         |  RETURN x + 2 AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("x", 10), Seq(varOf("x", 65), varOf("x", 65), varOf("x", 10)), Some("  x@0")),
      ExpectedSymbolGroup(varOf("x", 34), Seq(varOf("x", 45), varOf("x", 34)), Some("  x@1")),
      ExpectedSymbolGroup(varOf("y", 54), Seq(varOf("y", 68), varOf("y", 68), varOf("y", 54)), None)
    ))
  }

  test("""WITH 1 AS x
         |CALL () {
         |  WITH 2 AS x
         |  RETURN x + 2 AS y
         |  UNION
         |  WITH 2 AS x
         |  RETURN x + 2 AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("x", 10), Seq(varOf("x", 107), varOf("x", 107), varOf("x", 10)), Some("  x@0")),
      ExpectedSymbolGroup(varOf("x", 34), Seq(varOf("x", 45), varOf("x", 34)), Some("  x@1")),
      ExpectedSymbolGroup(varOf("y", 54), Seq(varOf("y", 54)), Some("  y@2")),
      ExpectedSymbolGroup(varOf("x", 76), Seq(varOf("x", 87), varOf("x", 76)), Some("  x@3")),
      ExpectedSymbolGroup(varOf("y", 96), Seq(varOf("y", 96)), Some("  y@4")),
      ExpectedSymbolGroup(varOf("y", 58), Seq(varOf("y", 110), varOf("y", 58), varOf("y", 110)), Some("  y@5"))
    ))
  }

  test("""WITH 1 AS x
         |CALL {
         |  WITH 2 AS x
         |  RETURN x + 2 AS y
         |  UNION
         |  WITH 2 AS x
         |  RETURN x + 2 AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("x", 10), Seq(varOf("x", 10), varOf("x", 104), varOf("x", 104)), Some("  x@0")),
      ExpectedSymbolGroup(varOf("x", 31), Seq(varOf("x", 42), varOf("x", 31)), Some("  x@1")),
      ExpectedSymbolGroup(varOf("y", 51), Seq(varOf("y", 51)), Some("  y@2")),
      ExpectedSymbolGroup(varOf("x", 73), Seq(varOf("x", 84), varOf("x", 73)), Some("  x@3")),
      ExpectedSymbolGroup(varOf("y", 93), Seq(varOf("y", 93)), Some("  y@4")),
      ExpectedSymbolGroup(varOf("y", 55), Seq(varOf("y", 55), varOf("y", 107), varOf("y", 107)), Some("  y@5"))
    ))
  }

  test("""WITH 1 AS x, 3 AS z
         |CALL {
         |  WITH 2 AS x, 5 AS z
         |  RETURN x + 2 + z AS y
         |  UNION
         |  WITH z
         |  WITH z, 2 AS x
         |  RETURN x + 2 + z AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("x", 10), Seq(varOf("x", 140), varOf("x", 140), varOf("x", 10)), Some("  x@0")),
      ExpectedSymbolGroup(
        varOf("z", 18),
        Seq(varOf("z", 97), varOf("z", 124), varOf("z", 88), varOf("z", 18), varOf("z", 88), varOf("z", 97)),
        Some("  z@1")
      ),
      ExpectedSymbolGroup(varOf("x", 39), Seq(varOf("x", 39), varOf("x", 58)), Some("  x@2")),
      ExpectedSymbolGroup(varOf("z", 47), Seq(varOf("z", 66), varOf("z", 47)), Some("  z@3")),
      ExpectedSymbolGroup(varOf("y", 71), Seq(varOf("y", 71)), Some("  y@4")),
      ExpectedSymbolGroup(varOf("x", 105), Seq(varOf("x", 116), varOf("x", 105)), Some("  x@5")),
      ExpectedSymbolGroup(varOf("y", 129), Seq(varOf("y", 129)), Some("  y@6")),
      ExpectedSymbolGroup(varOf("y", 75), Seq(varOf("y", 143), varOf("y", 143), varOf("y", 75)), Some("  y@7"))
    ))
  }

  test("""MATCH (x)((y)-[r:R]->(z))+
         |RETURN x, y, r, z""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("x", 7), Seq(varOf("x", 34), varOf("x", 34), varOf("x", 7)), None),
      ExpectedSymbolGroup(varOf("y", 11), Seq(varOf("y", 11), varOf("y", 11)), Some("  y@0")),
      ExpectedSymbolGroup(varOf("r", 15), Seq(varOf("r", 15), varOf("r", 15)), Some("  r@1")),
      ExpectedSymbolGroup(varOf("z", 22), Seq(varOf("z", 22), varOf("z", 22)), Some("  z@2")),
      ExpectedSymbolGroup(varOf("y", 9), Seq(varOf("y", 37), varOf("y", 37), varOf("y", 9)), Some("  y@3")),
      ExpectedSymbolGroup(varOf("r", 9), Seq(varOf("r", 40), varOf("r", 40), varOf("r", 9)), Some("  r@4")),
      ExpectedSymbolGroup(varOf("z", 9), Seq(varOf("z", 43), varOf("z", 9), varOf("z", 43)), Some("  z@5"))
    ))
  }

  test("""MATCH p = (n)
         |FOREACH (n IN nodes(p) | FOREACH (m IN nodes(p) | CREATE (n) ) )""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("p", 6), Seq(varOf("p", 59), varOf("p", 6), varOf("p", 34)), None),
      ExpectedSymbolGroup(varOf("n", 11), Seq(varOf("n", 11)), Some("  n@0")),
      ExpectedSymbolGroup(varOf("n", 23), Seq(varOf("n", 23)), Some("  n@1")),
      ExpectedSymbolGroup(varOf("m", 48), Seq(varOf("m", 48)), None),
      ExpectedSymbolGroup(varOf("n", 72), Seq(varOf("n", 72)), Some("  n@2"))
    ))
  }

  test("SHOW DEFAULT DATABASE YIELD name AS b SHOW DATABASES YIELD name RETURN *") {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("b", 36), Seq(varOf("b", 36)), None),
      ExpectedSymbolGroup(varOf("name", 59), Seq(varOf("name", 59), varOf("name", 59)), None)
    ))
  }

  test("""WITH {p: 1} AS a, 2 AS b
         |RETURN a.p AS x, sum(b) AS s
         |  GROUP BY a.p""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("a", 15), Seq(varOf("a", 65), varOf("a", 15)), None),
      ExpectedSymbolGroup(varOf("b", 23), Seq(varOf("b", 46), varOf("b", 23)), None),
      ExpectedSymbolGroup(varOf("x", 39), Seq(varOf("x", 39)), None),
      ExpectedSymbolGroup(varOf("s", 52), Seq(varOf("s", 52)), None)
    ))
  }

  test("""WITH {p: 1} AS a, {q: 2} AS e, 3 AS b
         |RETURN a.p + e.q AS x, sum(b) AS s
         |  GROUP BY a.p + e.q""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("a", 15), Seq(varOf("a", 15), varOf("a", 84)), None),
      ExpectedSymbolGroup(varOf("e", 28), Seq(varOf("e", 28), varOf("e", 90)), None),
      ExpectedSymbolGroup(varOf("b", 36), Seq(varOf("b", 65), varOf("b", 36)), None),
      ExpectedSymbolGroup(varOf("x", 58), Seq(varOf("x", 58)), None),
      ExpectedSymbolGroup(varOf("s", 71), Seq(varOf("s", 71)), None)
    ))
  }

  test("""WITH {p: 1} AS a, 2 AS b
         |CALL () {
         |  WITH {p: 3} AS a, 4 AS b
         |  RETURN a.p AS x, sum(b) AS s
         |    GROUP BY a.p
         |    ORDER BY a.p
         |}
         |RETURN a, b, x, s""".stripMargin) {
    hasSymbolGroups(Seq(
      ExpectedSymbolGroup(varOf("a", 15), Seq(varOf("a", 136), varOf("a", 15), varOf("a", 136)), Some("  a@0")),
      ExpectedSymbolGroup(varOf("b", 23), Seq(varOf("b", 23), varOf("b", 139), varOf("b", 139)), Some("  b@1")),
      ExpectedSymbolGroup(varOf("a", 52), Seq(varOf("a", 106), varOf("a", 52)), Some("  a@2")),
      ExpectedSymbolGroup(varOf("b", 60), Seq(varOf("b", 60), varOf("b", 85)), Some("  b@3")),
      ExpectedSymbolGroup(varOf("x", 78), Seq(varOf("x", 78), varOf("x", 142), varOf("x", 142)), None),
      ExpectedSymbolGroup(varOf("s", 91), Seq(varOf("s", 145), varOf("s", 145), varOf("s", 91)), None)
    ))
  }
}
