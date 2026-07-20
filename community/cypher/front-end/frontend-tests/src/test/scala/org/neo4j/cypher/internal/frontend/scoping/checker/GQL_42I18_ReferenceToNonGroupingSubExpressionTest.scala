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

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AggregationAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.NormalizeWithAndReturnClausesTransformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.scoping.E42I18
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoLocalCallableBody

/**
 * Test for 42I18 - Reference To Non-grouping Sub-expression
 */
class GQL_42I18_ReferenceToNonGroupingSubExpressionTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  // Thrown by AggregationChecker
  override val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] =
    Seq(NormalizeWithAndReturnClausesTransformer andThen ScopeSurveyor andThen AggregationAnalysis)

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """LET a = 10, b = 2, c = 5
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x / a) + b + c * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42I18("b", "c")),
      Seq("a", "s")
    ),
    TestQuery(
      """LET a = 10, b = 2, c = 5
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x/a) + x, SUM(x / a) + b + c * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42I18("x", "b", "c")),
      Seq("a", "`SUM(x/a) + x`", "s")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x / a) + x * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42I18("x")),
      Seq("a", "s")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS x
        |RETURN a AS x, SUM(x / a) + x * 5 AS s""".stripMargin,
      ignoreBeforeCypher25(E42I18("x")),
      Seq("x", "s")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: a.partner='Andres', kids: collect(child.name)}""".stripMargin,
      E42I18("a"),
      Seq("`a.name`", "`{foo: a.partner='Andres', kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: a.name=child.name, kids: collect(child.name)}""".stripMargin,
      E42I18("child"),
      Seq("`a.name`", "`{foo: a.name=child.name, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN {h:a.name}, {foo: {h:a.name}.h, kids: collect(child.name)}""".stripMargin,
      E42I18("a"),
      Seq("`{h:a.name}`", "`{foo: {h:a.name}.h, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN {h:a.name}, {foo: {h:a.name}.h, kids: collect(child.name)} ORDER BY {h:a.name}""".stripMargin,
      E42I18("a"),
      Seq("`{h:a.name}`", "`{foo: {h:a.name}.h, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN child.name, {foo: a.name='Andres', kids: collect(child.name)}""".stripMargin,
      E42I18("a"),
      Seq("`child.name`", "`{foo: a.name='Andres', kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a) RETURN COUNT { (a)--(b) } + count(a)""".stripMargin,
      E42I18("a"),
      Seq("`COUNT { (a)--(b) } + count(a)`")
    ),
    TestQuery(
      """WITH {a:1, b:{c:2}} AS map
        |RETURN map.b.c, map.b.c + count(*)""".stripMargin,
      E42I18("map"),
      Seq("`map.b.c`", "`map.b.c + count(*)`")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a, b AS m, count(*) AS cnt
        |  GROUP BY a""".stripMargin,
      ignoreBeforeCypher25(E42I18("b")),
      Seq("a", "m", "cnt")
    ),
    TestQuery(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |RETURN a, b + sum(c) AS s
        |  GROUP BY ALL""".stripMargin,
      ignoreBeforeCypher25(E42I18("b")),
      Seq("a", "s")
    ),

    // Positive tests
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: a.name='Andres', kids: collect(child.name)}""".stripMargin,
      Passes,
      Seq("`a.name`", "`{foo: a.name='Andres', kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: a.name=a.name, kids: collect(child.name)}""".stripMargin,
      Passes,
      Seq("`a.name`", "`{foo: a.name=a.name, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a, {foo: a=a, kids: collect(child.name)}""".stripMargin,
      Passes,
      Seq("a", "`{foo: a=a, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN {h:1}, {foo: {h:1}.h, kids: collect(child.name)}""".stripMargin,
      Passes,
      Seq("`{h:1}`", "`{foo: {h:1}.h, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN {h:a.name}, {foo: {h:1}.h, kids: collect(child.name)}""".stripMargin,
      Passes,
      Seq("`{h:a.name}`", "`{foo: {h:1}.h, kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY n.prop DESC
        |RETURN prop""".stripMargin,
      Passes,
      Seq("prop")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |RETURN a.name, {foo: a.name='Andres', kids: collect(child.name)} ORDER BY a.name""".stripMargin,
      Passes,
      Seq("`a.name`", "`{foo: a.name='Andres', kids: collect(child.name)}`")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |WITH a.name AS name, {foo: a.name='Andres', kids: collect(child.name)} AS map ORDER BY a.name
        |RETURN name, map""".stripMargin,
      Passes,
      Seq("name", "map")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |WITH a.name AS name, {foo: a.name='Andres', kids: collect(child.name)} AS map WHERE a.name
        |RETURN name, map""".stripMargin,
      Passes,
      Seq("name", "map")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |WITH a.name AS name, {foo: a.name='Andres', kids: collect(child.name)} AS map ORDER BY name, map
        |RETURN name, map""".stripMargin,
      Passes,
      Seq("name", "map")
    ),
    TestQuery(
      """MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
        |WITH a.name AS name, {foo: a.name='Andres', kids: collect(child.name)} AS map WHERE name AND map.foo
        |RETURN name, map""".stripMargin,
      Passes,
      Seq("name", "map")
    ),
    TestQuery(
      """WITH 1 AS m
        |MATCH (n)
        |RETURN m AS w, n, {f: m, q: SUM(n.age)} ORDER BY {f: m, q:SUM(n.age)}""".stripMargin,
      Passes,
      Seq("w", "n", "`{f: m, q: SUM(n.age)}`"),
      NoLocalCallableBody
    ),
    TestQuery(
      """WITH 1 AS g
        |RETURN COLLECT {
        |    UNWIND [1,2,3] AS x
        |    WITH * WHERE x < 0
        |    RETURN COUNT(*)+g AS agg
        |} AS x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1,2,3] as number
        |RETURN {
        |  a: max(number),
        |  b: min(number)
        |} AS map""".stripMargin,
      Passes,
      Seq("map")
    ),
    TestQuery(
      """MATCH (v:player)--(n:team)
        |RETURN n.age, [x IN collect(v.age) WHERE x > 40| x + n.age] AS res""".stripMargin,
      Passes,
      Seq("`n.age`", "res")
    ),
    TestQuery(
      """MATCH (v:player)--(n:team)
        |RETURN n.age, ANY(x IN collect(v.age) WHERE x > n.age) AS res""".stripMargin,
      Passes,
      Seq("`n.age`", "res")
    ),
    TestQuery(
      """MATCH (v:player)--(n:team)
        |RETURN n.age, reduce(acc = 0, x IN collect(v.age) | acc + x + n.age) AS res""".stripMargin,
      Passes,
      Seq("`n.age`", "res")
    ),
    TestQuery(
      """MATCH (v:player)--(n:team)
        |RETURN n.age, sum(size([(v)-[:KNOWS]->(p) WHERE p.age > n.age | p.age])) AS res""".stripMargin,
      Passes,
      Seq("`n.age`", "res")
    ),
    TestQuery(
      """MATCH (a:A)
        |CALL (a) {
        |  MATCH (a)-->(b:B)
        |  FILTER a.x = b.x
        |  RETURN COUNT(b) AS cntB
        |}
        |RETURN a, cntB""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "cntB")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS nums RETURN min(nums)""".stripMargin,
      Passes,
      Seq("`min(nums)`")
    )
  )
}
