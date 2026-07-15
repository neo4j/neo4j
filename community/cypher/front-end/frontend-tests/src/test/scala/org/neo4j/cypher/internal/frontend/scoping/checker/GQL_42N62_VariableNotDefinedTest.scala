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

import org.neo4j.cypher.internal.frontend.scoping.E42N62
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoLocalCallableBody

/**
 * Test for 42N62 - Variable Not Defined
 */
class GQL_42N62_VariableNotDefinedTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """UNWIND [1, 2, 3] AS a
        |RETURN x""".stripMargin,
      E42N62("x"),
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS a
        |CREATE (n:A {p: x})""".stripMargin,
      E42N62("x"),
      Seq.empty
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL () {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN b * x AS x
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN b * x AS x
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL (a) {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN b * x AS x
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  WITH a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN b * x AS x
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |CALL {
        |  WITH a
        |  WITH 2 AS b
        |  RETURN a AS x
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("a")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL (a) {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS y
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  WITH a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS y
        |}
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("a", "x")
    ),
    TestQuery(
      """FOREACH (a IN [1, 2, 3] | CREATE (n) )
        |RETURN a""".stripMargin,
      E42N62("a"),
      Seq("a")
    ),
    TestQuery(
      """FOREACH (a IN [1, 2, 3] | CREATE (n) )
        |RETURN n""".stripMargin,
      E42N62("n"),
      Seq("n")
    ),
    TestQuery(
      """FOREACH (a IN [1, 2, 3] | CREATE (n) SET x.p = a )""".stripMargin,
      E42N62("x"),
      Seq.empty
    ),
    TestQuery(
      """FOREACH (a IN [1, 2, 3] | SET x.p = a )""".stripMargin,
      E42N62("x"),
      Seq.empty
    ),
    TestQuery(
      """LET a = 10
        |FOREACH (b IN [1, 2, 3] | CREATE (n) SET n.p = x )
        |RETURN a""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("a")
    ),
    TestQuery(
      """CREATE (n {p: x})
        |RETURN n""".stripMargin,
      E42N62("x"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n)-[:R {p: x}]->()
        |RETURN n""".stripMargin,
      E42N62("x"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n)-[:R]->(), ({p: x})
        |RETURN n""".stripMargin,
      E42N62("x"),
      Seq("n")
    ),
    TestQuery(
      """CREATE (n:$all("A" + x) {p: 1})""".stripMargin,
      E42N62("x"),
      Seq.empty
    ),
    TestQuery(
      """RETURN 1 AS b
        |
        |NEXT
        |
        |RETURN x + 1 AS b""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("b")
    ),
    TestQuery(
      """LET x = 1
        |RETURN x AS a
        |
        |NEXT
        |
        |RETURN x + 1 AS b""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("b")
    ),
    TestQuery(
      """RETURN 1 AS x
        |
        |NEXT
        |
        |RETURN x AS a
        |
        |NEXT
        |
        |RETURN x + 1 AS b""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("b")
    ),
    TestQuery(
      """RETURN 1 AS a
        |
        |NEXT
        |{
        |  RETURN x + 1 AS a
        |  UNION
        |  RETURN a + 2 AS a
        |}""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("a")
    ),
    TestQuery(
      """LET x = 1
        |CALL (x) {
        |  RETURN x AS a
        |
        |  NEXT
        |
        |  RETURN x + 1 AS b, c AS a, a AS c
        |}
        |RETURN b, a, c""".stripMargin,
      ignoreBeforeCypher25(E42N62("c")),
      Seq("b", "a", "c")
    ),
    TestQuery(
      """RETURN 1 AS a
        |
        |NEXT
        |{
        |  RETURN a + 1 AS a
        |  UNION
        |  RETURN x + 2 AS a
        |}""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("a")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN x AS y
        |NEXT
        |WHEN y % 2 = 0 THEN RETURN x * -1 AS x
        |ELSE RETURN y AS x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN x AS y
        |NEXT
        |WHEN y % 2 = 0 THEN {
        |  LET a = -1
        |  RETURN y * a AS x
        |}
        |ELSE RETURN y * x AS x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN x AS y
        |NEXT
        |WHEN y % 2 = 0 THEN {
        |  LET x = -1
        |  RETURN y * x AS x
        |}
        |ELSE RETURN y * x AS x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """LET query = "bob"
        |CALL db.index.fulltext.queryNodes("myIndex", x) YIELD node, score
        |RETURN node ORDER BY score ASCENDING LIMIT 3""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("node")
    ),
    TestQuery(
      """LET query = "bob"
        |CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node, score
        |RETURN x ORDER BY score ASCENDING LIMIT 3""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """WITH "%s" AS g
        |CALL () {
        |    USE graph.byName(g)
        |    UNWIND [1, 2] AS i
        |    CREATE (:Number {value:i})
        |} IN TRANSACTIONS""".stripMargin,
      ignoreBeforeCypher25(E42N62("g")),
      Seq.empty
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |RETURN a AS g, SUM(x / a) + g * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42N62("g")),
      Seq("g", "s")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |RETURN a AS g, { prod: g * 5, agg: SUM(x / a) } AS s""".stripMargin,
      ignoreBeforeCypher25(E42N62("g")),
      Seq("g", "s")
    ),
    TestQuery(
      """LET a = 10, b = 2, c = 5
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x/a) + x, SUM(x / a) + b + g + c * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42N62("g")),
      Seq("g", "`SUM(x/a) + x`", "s")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |WITH a, SUM(x / a) + x * 5 AS s
        |RETURN x + 1 AS newX""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("newX")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x / a) + x * 5 AS s
        |
        |NEXT
        |
        |RETURN x + 1 AS newX""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("newX")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name AS x, {foo: x='Andres', kids: collect(child.name)}""".stripMargin,
      E42N62("x"),
      Seq("x", "`S{foo: x='Andres', kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN b.name, {kids: collect(child.name)}""".stripMargin,
      E42N62("b"),
      Seq("`b.name`", "`{kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN b.name, {foo: a.name=child.name, kids: collect(child.name)}""".stripMargin,
      E42N62("b"),
      Seq("`b.name`", "`{foo: a.name=child.name, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: b.name=child.name, kids: collect(child.name)}""".stripMargin,
      E42N62("b"),
      Seq("`a.name`", "`{foo: b.name=child.name, kids: collect(child.name)}`")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS a
        |CALL (a) {
        |  CALL (a, b) {
        |    RETURN 1 AS x
        |  }
        |  RETURN x
        |}
        |RETURN x""".stripMargin,
      E42N62("b"),
      Seq("x")
    ),
    TestQuery(
      """CALL (x) { RETURN 1 AS x } RETURN x""".stripMargin,
      E42N62("x"),
      Seq("x")
    ),
    TestQuery(
      """MATCH (n)-[:REL]->(d)
        |WHERE any(prefix in b WHERE n.name = prefix)
        |RETURN n.name""".stripMargin,
      E42N62("b"),
      Seq("`n.name`")
    ),
    TestQuery(
      """MATCH (n)-[:REL]->(d)
        |WHERE any(prefix in ["a", "b", "c"] WHERE b.name = prefix)
        |RETURN n.name""".stripMargin,
      E42N62("b"),
      Seq("`n.name`")
    ),
    TestQuery(
      """WITH "%s" AS g
        |CALL() {
        |  USE graph.byName(g)
        |  WITH 2 AS x
        |  RETURN *
        |}
        |RETURN x""".stripMargin,
      E42N62("g"),
      Seq("x")
    ),
    TestQuery(
      """RETURN 1 AS b NEXT RETURN x + 1 AS b""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("b")
    ),
    TestQuery(
      """MATCH (movie: Movie)
        |  SEARCH m IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(E42N62("m")),
      Seq("title", "score")
    ),
    TestQuery(
      """MATCH (movie: Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT l
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(E42N62("l")),
      Seq("title")
    ),
    TestQuery(
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    WHERE x.prop > 5
        |    LIMIT 5
        |  ) SCORE AS score
        |RETURN movie.title AS title, score""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("title", "score")
    ),
    TestQuery(
      """CREATE LOOKUP INDEX FOR (n) ON EACH labels(x)""".stripMargin,
      E42N62("x"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """CREATE FULLTEXT INDEX FOR (n:Label) ON EACH [x.prop]""".stripMargin,
      E42N62("x"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW USERS YIELD user ORDER BY bar ASCENDING""".stripMargin,
      E42N62("bar"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW USERS YIELD user ORDER BY passwordChangeRequired""".stripMargin,
      E42N62("passwordChangeRequired"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW SETTINGS foo""".stripMargin,
      E42N62("foo"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS YIELD nope""".stripMargin,
      E42N62("nope"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS foo""".stripMargin,
      E42N62("foo"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """TERMINATE TRANSACTIONS foo""".stripMargin,
      E42N62("foo"),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """CREATE VECTOR INDEX moviePlots IF NOT EXISTS
        |FOR (m:Movie) ON (m.embedding) WITH [m.releaseDate, m.rating]
        |OPTIONS {indexConfig: {`vector.similarity_function`: cosine}}""".stripMargin,
      ignoreBeforeCypher25(E42N62("cosine")),
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """MATCH (s)
        |  WHERE s.name = undefinedVariable AND s.age = 10
        |RETURN s""".stripMargin,
      E42N62("undefinedVariable"),
      Seq("s")
    ),
    TestQuery(
      """UNWIND graph.names() AS graphName
        |CALL () {
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id
        |}
        |CALL (id) {
        |    USE graph.byName( graphName )
        |    WITH id
        |    MATCH (n)
        |    WHERE elementId(n) = id
        |    DETACH DELETE n
        |} IN TRANSACTIONS""".stripMargin,
      E42N62("graphName"),
      Seq()
    ),
    TestQuery(
      """MATCH (a)
        |CALL {
        |    RETURN a.p + 1 AS y
        |}
        |RETURN y""".stripMargin,
      E42N62("a"),
      Seq()
    ),
    TestQuery(
      """MATCH (a)
        |RETURN a AS x, COUNT { CALL { RETURN a.p + 1 AS y } RETURN y } + count(a)""".stripMargin,
      E42N62("a"),
      Seq()
    ),
    TestQuery(
      """UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id, graphName AS newName
        |}
        |RETURN id, newName""".stripMargin,
      E42N62("graphName"),
      Seq("id", "newName")
    ),
    TestQuery(
      """WITH 1 AS x
        |UNWIND graph.names() AS graphName
        |CALL {
        |    WITH x
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y
        |}
        |RETURN id, y""".stripMargin,
      E42N62("graphName"),
      Seq("id", "y")
    ),
    TestQuery(
      """RETURN DISTINCT b""".stripMargin,
      E42N62("b"),
      Seq("b")
    ),
    TestQuery(
      """RETURN DISTINCT b, SUM(1) + b AS s""".stripMargin,
      E42N62("b"),
      Seq("b", "s")
    ),
    TestQuery(
      """WITH DISTINCT b
        |RETURN b""".stripMargin,
      E42N62("b"),
      Seq("b")
    ),
    TestQuery(
      """WITH DISTINCT b, SUM(1) + b AS s
        |RETURN b""".stripMargin,
      E42N62("b"),
      Seq("b")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN DISTINCT bacon, a, p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, COUNT(p) AS cnt""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "cnt")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH bacon, a, p
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH DISTINCT bacon, a, p
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH bacon, a, COUNT(p) AS cnt
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "cnt")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, 2 * p AS p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN DISTINCT bacon, a, 2 * p AS p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, COUNT(2 * p) AS cnt""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "cnt")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH bacon, a, 2 * p AS p
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH DISTINCT bacon, a, 2 * p AS p
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH bacon, a, COUNT(2 * p) AS cnt
        |RETURN *""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "cnt")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, [x IN a.list | p * x] AS p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN DISTINCT bacon, a, [x IN a.list | p * x] AS p""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, COUNT([x IN a.list | p * x]) AS cnt""".stripMargin,
      E42N62("p"),
      Seq("bacon", "a", "cnt")
    ),
    TestQuery(
      """MATCH (issue:DataQualityIssue)
        |OPTIONAL MATCH (source)-[:HAS_DQ_ISSUE]->(issue)
        |WITH issue, source, labels(source)[0] AS sourceLabel
        |WITH coalesce(
        |       CASE WHEN sourceLabel = "A" THEN source.name
        |            WHEN sourceLabel = "B" THEN source.name
        |            WHEN sourceLabel = "C" THEN source.name
        |            WHEN sourceLabel = "D" THEN "D" + source.orderId
        |            WHEN sourceLabel = "E" THEN source.sourceSystem + "E" + source.objectType
        |            WHEN sourceLabel = "F" THEN source.goldenName
        |            ELSE "G" END, "else") AS hotspot,
        |     coalesce(sourceLabel, "null") AS hotspotType,
        |     issue
        |RETURN DISTINCT issue AS _graph_issue, source AS _graph_source
        |LIMIT 10""".stripMargin,
      E42N62("source"),
      Seq("_graph_issue", "_graph_source")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})
        |WHERE COUNT {
        |  MATCH (owner)-[*1..3]-(a:Person)
        |  WITH count(*) AS num, owner AS owner, p
        |  RETURN num
        |} = 1
        |RETURN owner""".stripMargin,
      E42N62("p"),
      Seq("owner")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})
        |WHERE COUNT {
        |  MATCH (owner)-[*1..3]-(a:Person)
        |  RETURN DISTINCT owner, p
        |} = 1
        |RETURN owner""".stripMargin,
      E42N62("p"),
      Seq("owner")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})
        |WHERE COUNT {
        |  MATCH (owner)-[*1..3]-(a:Person)
        |  WITH count(*) AS num
        |    GROUP BY owner, p
        |  RETURN num
        |} = 1
        |RETURN owner""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("owner")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})
        |WHERE COUNT {
        |  MATCH (owner)-[*1..3]-(a:Person)
        |  RETURN count(*) AS num
        |    GROUP BY owner, p
        |} = 1
        |RETURN owner""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("owner")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name + x AS subPet
        |       } AS numPets""".stripMargin,
      E42N62("x"),
      Seq("outerCnt", "numPets")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS subPet
        |       } AS numPets
        |  GROUP BY COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name + x AS subPet
        |       }""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("outerCnt", "numPets")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, p
        |  GROUP BY bacon, a, p""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, 2 * p AS p
        |  GROUP BY bacon, a, 2 * p""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |WITH bacon, a, 2 * p AS p
        |  GROUP BY bacon, a, 2 * p
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """MATCH (bacon:Person {name:'Bob'})-[*1..3]-(a:Person)
        |RETURN bacon, a, [x IN a.list | p * x] AS p
        |  GROUP BY bacon, a, [x IN a.list | p * x]""".stripMargin,
      ignoreBeforeCypher25(E42N62("p")),
      Seq("bacon", "a", "p")
    ),
    TestQuery(
      """RETURN 1 AS b
        |  GROUP BY x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("b")
    ),
    TestQuery(
      """RETURN x
        |  GROUP BY ()""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """RETURN x
        |  GROUP BY x""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """RETURN x
        |  GROUP BY ALL""".stripMargin,
      ignoreBeforeCypher25(E42N62("x")),
      Seq("x")
    ),
    TestQuery(
      """WITH 1 AS x
        |RETURN x + y AS a, count(*) AS c
        |  GROUP BY ALL""".stripMargin,
      ignoreBeforeCypher25(E42N62("y")),
      Seq("a", "c")
    ),
    TestQuery(
      """WITH 1 AS x
        |RETURN x AS a, y AS b, count(*) AS c
        |  GROUP BY ALL""".stripMargin,
      ignoreBeforeCypher25(E42N62("y")),
      Seq("a", "b", "c")
    ),

    // Positive tests
    TestQuery(
      """LET a = 10
        |CALL {
        |  WITH a
        |  WITH 2 AS a
        |  RETURN a AS x
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS i
        |WITH i ORDER BY i DESC
        |CALL (i) {
        |  MATCH (n {value: i})
        |  CREATE (m {value: i - 1})
        |  FINISH
        |}
        |RETURN count(*) as count""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("count")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL (a) {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS OF 2 + a ROWS ON ERROR RETRY FOR 2.5 SECONDS
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  WITH a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS OF 2 + a ROWS ON ERROR RETRY FOR 2.5 SECONDS
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL (a) {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY FOR a + 1 SECONDS
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  WITH a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY FOR a + 1 SECONDS
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "x")
    ),
    TestQuery(
      """MATCH (n {p: n.p})
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """MATCH (n {p: 1})-[r:R WHERE r.p > 1]->()
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """MATCH (n {p: 1})-[:R]->({p: n.p})
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """WITH 1 AS x
        |RETURN x AS name
        |LIMIT x""".stripMargin,
      Passes,
      Seq("name")
    ),
    TestQuery(
      """WITH 1 AS x
        |RETURN x AS name
        |SKIP x""".stripMargin,
      Passes,
      Seq("name")
    ),
    TestQuery(
      """CREATE (a:A {name: 'a'}),
        |       (b1:X {name: 'b1'})
        |CREATE (a)-[:KNOWS]->(b1)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (:A)-[m:R {p:'hello'}]->(:B)
        |CREATE (c:C {t:type(m), p:m.p}) RETURN m, c""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """UNWIND [1, 2] as i
        |CREATE (:A)
        |CREATE (n:B {id: i, count: COUNT { MATCH (:A) } })
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """FOREACH(v IN [null] | CREATE ({property: v}))""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """FOREACH(x IN [1, 2, 3] | MERGE ({property: x}))""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (a)-[r]->+(b)
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.prop, rel.prop = 5)
        |RETURN a, b""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH (n)-[:REL]->(d)
        |WHERE any(d in ["a", "b", "c"] WHERE n.name = d)
        |RETURN n.name""".stripMargin,
      Passes,
      Seq("`n.name`")
    ),
    TestQuery(
      """MATCH (n)-[:REL]->(d)
        |WHERE any(prefix in ["a", "b", "c"] WHERE n.name = prefix)
        |RETURN n.name""".stripMargin,
      Passes,
      Seq("`n.name`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN [x=(n)-->() | head(nodes(x))] AS p""".stripMargin,
      Passes,
      Seq("p")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN [(n)-->() | n.name] AS p""".stripMargin,
      Passes,
      Seq("p")
    ),
    TestQuery(
      """WITH 1 AS x
        |MATCH (a:A WHERE a.prop > x)-[r]-(b:B)
        |RETURN a, r, b""".stripMargin,
      Passes,
      Seq("a", "r", "b")
    ),
    TestQuery(
      """WITH 1 AS x
        |RETURN [(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop] AS result""".stripMargin,
      Passes,
      Seq("result")
    ),
    TestQuery(
      """MATCH (a:P)
        |RETURN
        |  CASE COLLECT { CALL (a) { RETURN a.p AS p } RETURN p }[0]
        |    WHEN > 2 THEN '> 2'
        |    ELSE 'else'
        |  END AS res
        |ORDER BY res""".stripMargin,
      Passes,
      Seq("res")
    ),
    TestQuery(
      """MATCH SHORTEST $one (p = ((start)((a)-[r:R]->(b))+(end))
        |  WHERE length(p) > 3)
        |RETURN p""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p")
    ),
    TestQuery(
      """MATCH (movie:Movie)
        |  WHERE score > 0
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  ) SCORE AS score
        |RETURN movie.title AS title, score""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("title", "score")
    ),
    TestQuery(
      """WITH 1 AS l
        |MATCH (movie: Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT l
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("title")
    ),
    TestQuery(
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    WHERE movie.prop > 5
        |    LIMIT 5
        |  ) SCORE AS score
        |RETURN movie.title AS title, score""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("title", "score")
    ),
    TestQuery(
      """MATCH (m: Movie {title: 'Cinderella'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    WHERE movie.prop > m.prop
        |    LIMIT 5
        |  ) SCORE AS score
        |RETURN movie.title AS title, score""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("title", "score")
    ),
    TestQuery(
      """SHOW USERS WHERE user ='bob'""".stripMargin,
      Passes,
      Seq("user", "roles", "passwordChangeRequired", "suspended", "home"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE""".stripMargin,
      Passes,
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """CREATE INDEX FOR (n:Person) ON (n.firstName)""".stripMargin,
      Passes,
      Seq.empty,
      compositionRestriction = NoLocalCallableBody
    ),
    // this test currently fails with
    // Invalid input 'SHOW': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF> (line 3, column 1 (offset: 36))
    // "mock"
    // TODO: Fix
    /*

    TestQuery(
      """SHOW INDEXES
        |YIELD type, entityType
        |SHOW RANGE INDEXES
        |YIELD name AS range
        |RETURN *
        |ORDER BY type, entityType""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("type", "entityType", "range"),
      compositionRestriction = NoLocalCallableBody
    ),
     */
    TestQuery(
      """SHOW TRANSACTIONS""".stripMargin,
      Passes,
      Seq(
        "database",
        "transactionId",
        "currentQueryId",
        "connectionId",
        "clientAddress",
        "username",
        "currentQuery",
        "startTime",
        "status",
        "elapsedTime"
      ),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS WHERE database <> 'system'""".stripMargin,
      Passes,
      Seq(
        "database",
        "transactionId",
        "currentQueryId",
        "connectionId",
        "clientAddress",
        "username",
        "currentQuery",
        "startTime",
        "status",
        "elapsedTime"
      ),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS YIELD * WHERE database <> 'system'""".stripMargin,
      Passes,
      Seq(
        "database",
        "transactionId",
        "currentQueryId",
        "connectionId",
        "clientAddress",
        "username",
        "currentQuery",
        "startTime",
        "status",
        "elapsedTime"
      ),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTION 'neo4j-transaction-2'
        |YIELD transactionId
        |TERMINATE TRANSACTION transactionId
        |YIELD message, transactionId AS txId, username
        |RETURN *""".stripMargin,
      Passes,
      Seq("transactionId", "message", "txId", "username"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS
        |YIELD username AS user, database, transactionId AS txId, status AS status
        |ORDER BY username, txId, status
        |WHERE username = $userParam AND txId IS NOT NULL AND status >= ''
        |RETURN user, database, txId""".stripMargin,
      Passes,
      Seq("user", "database", "txId"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS
        |YIELD username AS user, database, transactionId AS txId, status AS status
        |ORDER BY username, txId, status
        |WHERE username = $userParam AND txId IS NOT NULL AND status >= ''
        |RETURN *""".stripMargin,
      Passes,
      Seq("user", "database", "txId", "status"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS "neo4j-transaction-2"
        |YIELD transactionId
        |TERMINATE TRANSACTIONS transactionId
        |YIELD message, transactionId AS txId, username
        |SHOW TRANSACTIONS txId
        |YIELD username AS user
        |RETURN *""".stripMargin,
      Passes,
      Seq("transactionId", "message", "txId", "username", "user"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS "neo4j-transaction-2"
        |YIELD transactionId
        |TERMINATE TRANSACTIONS transactionId
        |YIELD message, transactionId AS txId, username
        |SHOW TRANSACTIONS txId
        |YIELD username AS user
        |WHERE username = ""
        |RETURN *""".stripMargin,
      Passes,
      Seq("transactionId", "message", "txId", "username", "user"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS YIELD database
        |SHOW TRANSACTIONS YIELD username
        |WHERE database = "neo4j"
        |RETURN *""".stripMargin,
      Passes,
      Seq("database", "username"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTIONS
        |YIELD database AS db1
        |  WHERE db1 <> "system"
        |SHOW TRANSACTIONS
        |YIELD database AS db2
        |  WHERE db2 <> "system"
        |RETURN *""".stripMargin,
      Passes,
      Seq("db1", "db2"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """SHOW TRANSACTION
        |YIELD transactionId AS txId, parameters AS params
        |WHERE NOT isEmpty(parameters)
        |SHOW FUNCTIONS
        |YIELD name AS function
        |ORDER BY name
        |LIMIT 5
        |SHOW INDEXES
        |YIELD type AS indexType, entityType AS indexEntityType
        |ORDER BY type
        |WHERE entityType = 'NODE'
        |SHOW CURRENT GRAPH TYPE
        |YIELD specification AS spec
        |WHERE specification <> ''
        |TERMINATE TRANSACTION txId
        |YIELD message AS status
        |SHOW SETTING params.setting
        |YIELD name AS setting, value
        |ORDER BY name
        |SHOW PROCEDURES
        |YIELD name AS procedure, admin AS admin
        |WHERE NOT admin AND name >= ''
        |SHOW CONSTRAINTS
        |YIELD name AS constraint, labelsOrTypes AS name
        |ORDER BY name
        |RETURN name, txId, indexEntityType, spec, function
        |LIMIT 1""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("name", "txId", "indexEntityType", "spec", "function"),
      compositionRestriction = NoLocalCallableBody
    ),
    TestQuery(
      """UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id
        |}
        |CALL {
        |    USE graph.byName( graphName )
        |    WITH id
        |    MATCH (n)
        |    WHERE elementId(n) = id
        |    DETACH DELETE n
        |} IN TRANSACTIONS""".stripMargin,
      Passes,
      Seq()
    ),
    TestQuery(
      """UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id
        |}
        |WITH graphName, id
        |CALL {
        |    USE graph.byName( graphName )
        |    WITH id
        |    MATCH (n)
        |    WHERE elementId(n) = id
        |    DETACH DELETE n
        |} IN TRANSACTIONS""".stripMargin,
      Passes,
      Seq()
    ),
    TestQuery(
      """UNWIND [1,2,3] AS value
        |CALL {
        |    WITH value
        |    MATCH (n)
        |    RETURN value + 1 AS inc, elementId(n) AS id
        |}
        |WITH value, id, inc
        |CALL {
        |    WITH value, id, inc
        |    WITH *, value + inc + 2 AS inc2
        |    MATCH (n)
        |    WHERE elementId(n) = id
        |    DETACH DELETE n
        |} IN TRANSACTIONS""".stripMargin,
      Passes,
      Seq()
    ),
    TestQuery(
      """UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    MATCH (n)
        |    RETURN elementId(n) AS id
        |}
        |RETURN id""".stripMargin,
      Passes,
      Seq("id")
    ),
    TestQuery(
      """WITH 1 AS x
        |UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    WITH x
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y
        |}
        |RETURN id, y""".stripMargin,
      Passes,
      Seq("id", "y")
    ),
    TestQuery(
      """WITH 1 AS x, "name" AS stringName
        |UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    WITH x
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y
        |    UNION
        |    USE graph.byName( stringName )
        |    WITH x
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y
        |}
        |RETURN id, y""".stripMargin,
      Passes,
      Seq("id", "y")
    ),
    TestQuery(
      """WITH 1 AS x, "name" AS stringName
        |UNWIND graph.names() AS graphName
        |CALL {
        |    USE graph.byName( graphName )
        |    WITH x
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y, "hello" AS newString
        |    UNION
        |    USE graph.byName( stringName )
        |    WITH x, stringName
        |    MATCH (n)
        |    RETURN elementId(n) AS id, x + 1 AS y, stringName AS newString
        |}
        |RETURN id, y, newString""".stripMargin,
      Passes,
      Seq("id", "y", "newString")
    ),
    TestQuery(
      """MATCH (a)
        |CALL {
        |    WITH *
        |    RETURN a.p + 1 AS y
        |}
        |RETURN y""".stripMargin,
      Passes,
      Seq("y")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y, 3 AS z
        |RETURN x + y AS a
        | GROUP BY a, z""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y, {p: 1} AS z
        |RETURN x, z.p AS a
        |  GROUP BY a, y + 1""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x", "a")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y, 3 AS z
        |RETURN x + y AS a
        |  GROUP BY a, z""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y
        |RETURN x + y AS z
        |  GROUP BY z""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("z")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y, 4 AS z
        |RETURN x + y AS z
        |  GROUP BY z""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("z")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS y
        |RETURN x, y, x + y AS z
        |  GROUP BY x, y""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x", "y", "z")
    ),
    TestQuery(
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt
        |  GROUP BY a, b""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "b", "cnt")
    ),
    TestQuery(
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt
        |  GROUP BY ALL""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "b", "cnt")
    ),
    TestQuery(
      """WITH 1 AS a, 2 AS b
        |RETURN *
        |  GROUP BY a, b""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "b")
    ),
    TestQuery(
      """WITH 1 AS a, 2 AS b
        |WITH *, count(*) AS cnt
        |  GROUP BY a, b
        |RETURN cnt""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("cnt")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY s + SUM(x) ASCENDING""".stripMargin,
      Passes,
      Seq("s")
    )
  )
}
