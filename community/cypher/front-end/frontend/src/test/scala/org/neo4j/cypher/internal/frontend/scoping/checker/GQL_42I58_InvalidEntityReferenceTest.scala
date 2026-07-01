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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.E42I58
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.Versioned.passesBeforeCypher25

/**
 * Test for 42I58 - Invalid Entity Reference
 */
class GQL_42I58_InvalidEntityReferenceTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = testCasesCreate ++ testCasesInsert ++ testCasesMerge

  private def testCasesCreate = Seq(
    TestQuery(
      """CREATE (a)-[:REL]->(b {prop: a.prop})""",
      E42I58("a"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (b)-[:A]->()-[:B]->({x: b.p}), (a)""",
      E42I58("b"),
      Seq.empty
    ),
    TestQuery(
      """CREATE ({x: b.p})-[:A]->()-[:B]->(b), (a)""",
      E42I58("b"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)-[r:REL]->(b {prop: r.prop})""",
      E42I58("r"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n:$(n.prop) {prop:5})""",
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE ()-[r:$(r.prop) {prop:5}]->()""",
      E42I58("r"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n)-[r:R {p: 1}]->({p: r.p})
        |RETURN n""".stripMargin,
      E42I58("r"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n {p: 1})-[:R]->({p: n.p})
        |RETURN n""".stripMargin,
      E42I58("n"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n {p: n.p})-[:R]->({p: 1})
        |RETURN n""".stripMargin,
      E42I58("n"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n {p: 1})-[:R]->(:$all("A" + n.p) {p: 1})""".stripMargin,
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a:A), (a)-[r:R {p: a.t}]->(b:B {p: a.q})
        |RETURN a""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """CREATE (a:A {p: 1})-[r:R {p: 1}]->(b:B {p: 1}), (c:C)-[s:S {p: a.p+b.p+r.p}]->(d:D)
        |RETURN a""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """INSERT (a)-[:REL]->(b {prop: a.prop})""".stripMargin,
      E42I58("a"),
      Seq.empty
    ),
    TestQuery(
      """INSERT (a)-[r:REL]->(b {prop: r.prop})""".stripMargin,
      E42I58("r"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: EXISTS{ MATCH (n) RETURN n.prop}})""".stripMargin,
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (x) CREATE (a)-[r:R]->(b {prop: EXISTS { (a)-[r2]->(c) }})""".stripMargin,
      E42I58("a"),
      Seq.empty
    ),
    TestQuery(
      """MATCH (prev)
        |WITH prev, 5 AS five
        |MERGE (prev)-[r:R]->(a)-[r2:R {p:r.p}]->(b)""".stripMargin,
      passesBeforeCypher25(E42I58("r")),
      Seq.empty
    )
  ) ++ (
    for {
      scalaSubquery <- Seq(
        "EXISTS { MATCH (c) WHERE c.prop = a.prop }",
        "COUNT { MATCH (c) WHERE c.prop = a.prop }",
        "COLLECT { MATCH (c) WHERE c.prop = a.prop RETURN c }"
      )
      (patternA, patternB) = ("(a {prop: true})", s"(b {prop: $scalaSubquery})")
      pattern <- Seq(
        s"$patternA, $patternB",
        s"$patternB, $patternA"
      )
    } yield {
      TestQuery(
        s"""CREATE $pattern""".stripMargin,
        E42I58("a"),
        Seq.empty
      )
    }
  ) ++ Seq(
    TestQuery(
      """CREATE (a), (b {prop: a.prop})""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)-[r:REL]->(b), (c {prop: r.prop})""".stripMargin,
      passesBeforeCypher25(E42I58("r")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a:A), (a)-[r:R {p: a.q}]->(b:B {p: a.q})
        |RETURN a""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """CREATE (a:A {p: 2})-[r:R {p: 1}]->(b:B {p: 1}), (c:C)-[s:S {p: a.p+b.p+r.p}]->(d:D)
        |RETURN a""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """CREATE (n {p: 1}), ({p: n.p})
        |RETURN n""".stripMargin,
      passesBeforeCypher25(E42I58("n")),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n)-[r:R {p: 1}]->(), ({p: r.p})
        |RETURN n""".stripMargin,
      passesBeforeCypher25(E42I58("r")),
      Seq("n")
    ),
    TestQuery(
      """CREATE p = (n)-[r:R {p: 1}]->(), ({p: length(p)})
        |RETURN n""".stripMargin,
      passesBeforeCypher25(E42I58("p")),
      Seq("n")
    ),
    TestQuery(
      """CREATE (a {prop: 1}), ({prop: a.prop})""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (b {prop: a.prop}), (a)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (b {prop: EXISTS {(a)-->()}}), (a)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE ()-[:A]->()-[:B]->({x: a.p}), (a)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |CREATE ()-[:A {p: n.p}]->(n)-[:B]->({x: a.p}), (a)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (x) CREATE (a), (a1)-[r:R]->(b {prop: EXISTS { (a)-[r2]->(c) }})""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (x) CREATE (a), (a1)-[r:R {prop: EXISTS { (a)-[r2]->(c) }}]->(b)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: true IN [x IN [false] | n]})""".stripMargin,
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: any(x IN [false] WHERE n)})""".stripMargin,
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: reduce(x = 1, y in [1,2] | n + x + y)})""".stripMargin,
      E42I58("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: allReduce(x = 0, y IN [1] | x + n, x < 5)})""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: true IN [n IN [false] | n]})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: any(n IN [false] WHERE n)})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: reduce(n = 1, x in [1,2] | n + x)})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (n {prop: allReduce(n = 0, x IN [1] | n + x, n < 5)})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    )
  )

  private def testCasesInsert = Seq(
    TestQuery(
      """INSERT (a), (b {prop: a.prop})""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (a)-[r:REL]->(b), (c {prop: r.prop})""".stripMargin,
      ignoreBeforeCypher25(E42I58("r")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (a:A), (a)-[r:R {p: a.q}]->(b:B {p: a.q})
        |RETURN a""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """INSERT (a:A {p: 2})-[r:R {p: 1}]->(b:B {p: 1}), (c:C)-[s:S {p: a.p+b.p+r.p}]->(d:D)
        |RETURN a""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq("a")
    ),
    TestQuery(
      """INSERT (n {p: 1}), ({p: n.p})
        |RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq("n")
    ),
    TestQuery(
      """INSERT (n)-[r:R {p: 1}]->(), ({p: r.p})
        |RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42I58("r")),
      Seq("n")
    ),
    TestQuery(
      """INSERT (a {prop: 1}), ({prop: a.prop})""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (x) INSERT (a)-[r:R]->(b {prop: EXISTS { (a)-[r2]->(c) }})""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (x) INSERT (a)-[r:R {prop: EXISTS { (a)-[r2]->(c) }}]->(b)""".stripMargin,
      ignoreBeforeCypher25(E42I58("a")),
      Seq.empty
    )
  ) ++ (
    for {
      scalaSubquery <- Seq(
        "EXISTS { MATCH (c) WHERE c.prop = a.prop }",
        "COUNT { MATCH (c) WHERE c.prop = a.prop }",
        "COLLECT { MATCH (c) WHERE c.prop = a.prop RETURN c }"
      )
      (patternA, patternB) = ("(a {prop: true})", s"(b {prop: $scalaSubquery})")
      pattern <- Seq(
        s"$patternA, $patternB",
        s"$patternB, $patternA"
      )
    } yield {
      TestQuery(
        s"""INSERT $pattern""".stripMargin,
        ignoreBeforeCypher25(E42I58("a")),
        Seq.empty
      )
    }
  ) ++ Seq(
    TestQuery(
      """INSERT (n {prop: true IN [x IN [false] | n]})""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: reduce(x = 1, y in [1,2] | n + x + y)})""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: any(x IN [false] WHERE n)})""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: allReduce(x = 0, y IN [1] | x + n, x < 5)})""".stripMargin,
      ignoreBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: true IN [n IN [false] | n]})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: reduce(n = 1, x in [1,2] | n + x)})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: reduce(x = 1, n in [1,2] | n + x)})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: any(n IN [false] WHERE n)})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: allReduce(n = 0, x IN [1] | n + x, n < 5)})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    ),
    TestQuery(
      """INSERT (n {prop: allReduce(x = 0, n IN [1] | x + n, x < 5)})""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq.empty
    )
  )

  private def testCasesMerge = Seq(
    TestQuery(
      """MERGE (a {prop:'p'})-[:T]->(b {prop:a.prop})""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    ),
    TestQuery(
      """MERGE (n)-[r:R {p: 1}]->({p: r.p})
        |RETURN n""".stripMargin,
      passesBeforeCypher25(E42I58("r")),
      Seq("n")
    ),
    TestQuery(
      """MERGE (n {p: 1})-[:R]->({p: n.p})
        |RETURN n""".stripMargin,
      passesBeforeCypher25(E42I58("n")),
      Seq("n")
    ),
    TestQuery(
      """MERGE (n {p: 1})-[:R]->(:$all("A" + n.p) {p: 1})""".stripMargin,
      passesBeforeCypher25(E42I58("n")),
      Seq.empty
    ),
    TestQuery(
      """MERGE (prev)-[r:R {p: a.p}]->(a)""".stripMargin,
      passesBeforeCypher25(E42I58("a")),
      Seq.empty
    )
  )
}
