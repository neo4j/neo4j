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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Asserts that `Namespacer.processOld` and `Namespacer.processNew` agree on
 * a curated set of queries. Equivalence is checked after canonicalising
 * generator-assigned variable names so only the *shape* of the renaming
 * (equivalence classes and rename decisions) is compared.
 *
 * If a case fails here the two algorithms genuinely diverge; cosmetic
 * ordering differences in anonymous name counters are filtered out by the
 * canonicaliser in [[NamespacerParityTestSupport]].
 */
class NamespacerParityTest
    extends CypherFunSuite
    with AstConstructionTestSupport
    with RewritePhaseTest
    with NamespacerParityTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] = NoOp()

  override val phaseTestConfig: PhaseTestConfig =
    PhaseTestConfig(semanticFeatures = Seq(SemanticFeature.ComposableCommands))

  // Use the production step-sequenced pre-Namespacer pipeline so the state we hand to
  // processOld / processNew matches what the real compiler would produce.
  override def preProcessTransformer: Transformer[BaseContext, BaseState, BaseState] =
    FrontEndCompilationPhases.parsing(
      ParsingConfig(StrictResolveCallables.NoResolver)
    ) andThen AstRewriting() andThen SemanticAnalysis(Some(false))

  test("no renamings required") {
    assertParity("MATCH (n) RETURN n AS n")
  }

  test("simple shadow across MATCH clauses") {
    assertParity("MATCH (n), (x) WITH n AS n MATCH (x) RETURN n AS n, x AS x")
  }

  test("shadow with property access") {
    assertParity("MATCH (a) WITH a.name AS n ORDER BY a.foo MATCH (a) RETURN a.age")
  }

  test("EXISTS subquery with outer shadow") {
    assertParity("MATCH (n) WHERE EXISTS { MATCH (n)-[r]->(p) } WITH n AS m, 1 AS n RETURN m, n")
  }

  test("COUNT subquery with outer shadow") {
    assertParity("MATCH (n) WHERE COUNT { (n)-[r]->(p) } > 1 WITH n AS m, 1 AS n RETURN m, n")
  }

  test("reduce introduces scope dependencies and accumulator") {
    assertParity(
      """WITH [1,2,3] AS nums
        |RETURN reduce(weight = 0, num IN nums | weight + num) AS weight
        |ORDER BY weight DESC""".stripMargin
    )
  }

  test("any/all introduce scoped variables") {
    assertParity(
      """MATCH (n)
        |WHERE any(x IN [1,2,3] WHERE x > 1) AND all(x IN [4,5,6] WHERE x > 0)
        |RETURN n""".stripMargin
    )
  }

  test("FOREACH with reused outer name") {
    assertParity("WITH 1 AS foo FOREACH (foo IN [1,2,3] | CREATE (c)) RETURN foo AS bar ORDER BY foo")
  }

  test("FOREACH introduces new name that collides with return alias") {
    assertParity("WITH 1 AS foo FOREACH (bar IN [1,2,3] | CREATE (c)) RETURN foo AS bar ORDER BY bar")
  }

  test("FOREACH-local CREATE re-binds outer scoped name to a new node") {
    assertParity(
      """WITH 1 AS target
        |FOREACH (_ IN CASE WHEN target IS NULL THEN [0] ELSE [] END |
        |  CREATE (target:Lead) SET target.createdAt = 1
        |)
        |WITH 1 AS target
        |RETURN target""".stripMargin
    )
  }

  test("FOREACH-local SET on outer node uses outer scope") {
    assertParity(
      """MATCH (a)
        |FOREACH (_ IN [1] |
        |  SET a.foo = 1
        |)
        |WITH 1 AS a
        |RETURN a""".stripMargin
    )
  }

  test("Nested FOREACH where inner CREATE re-uses outer FOREACH iter name") {
    assertParity(
      "MATCH p = (n) FOREACH (n IN nodes(p) | FOREACH (m IN nodes(p) | CREATE (n) ) ) WITH 1 AS m, 1 AS n RETURN *"
    )
  }

  test("FOREACH-local MERGE with outer-name") {
    assertParity(
      """MATCH (a)
        |FOREACH (_ IN [1] |
        |  MERGE (a:Extra)
        |)
        |WITH 1 AS a
        |RETURN a""".stripMargin
    )
  }

  test("UNION with overlapping variable names") {
    assertParity(
      """MATCH (n)
        |RETURN n AS x
        |UNION
        |MATCH (n)
        |RETURN n AS x""".stripMargin
    )
  }

  test("UNION ALL with inner shadow") {
    assertParity(
      """MATCH (n), (x) WITH n AS n MATCH (x) RETURN n AS a, x AS b
        |UNION ALL
        |MATCH (n) RETURN n AS a, n AS b""".stripMargin
    )
  }

  test("WITH * UNWIND then ORDER BY reduce") {
    assertParity(
      """CREATE (a)-[:T]->(b)
        |WITH *
        |UNWIND labels(a) + labels(b) AS label
        |RETURN 1 ORDER BY reduce(a = 1, b IN [] | 1)""".stripMargin
    )
  }

  test("QPP with shadowed boundary variable") {
    assertParity(
      "MATCH (a) ((x)-->(y))+ WHERE x = 0 WITH '1' AS x WHERE y IS NOT NULL RETURN x"
    )
  }

  test("QPP then outer WITH rebinds group variable name") {
    assertParity(
      "MATCH ((x)-->(y)){1,3} WITH collect(x) AS xs, 1 AS x RETURN xs, x"
    )
  }

  // --- Nested-clause corpus --------------------------------------------------

  test("chained UNION with same-name output column") {
    assertParity(
      """MATCH (n) RETURN n AS x
        |UNION
        |MATCH (n) RETURN n AS x
        |UNION
        |MATCH (n) RETURN n AS x""".stripMargin
    )
  }

  test("CALL subquery containing UNION, outer shadow of mapped column") {
    assertParity(
      """MATCH (x)
        |CALL {
        |  WITH x MATCH (x)-[:R]->(m) RETURN m AS y
        |  UNION
        |  WITH x MATCH (x)<-[:R]-(m) RETURN m AS y
        |}
        |RETURN x, y""".stripMargin
    )
  }

  test("UNION inside EXISTS with outer variable shadowed in each branch") {
    assertParity(
      """MATCH (n)
        |WHERE EXISTS {
        |  MATCH (n)-[:R]->(m) RETURN m
        |  UNION
        |  MATCH (n)<-[:R]-(m) RETURN m
        |}
        |RETURN n""".stripMargin
    )
  }

  test("CALL importing a shadowed variable") {
    assertParity(
      """MATCH (x)
        |CALL {
        |  WITH x MATCH (x)-[:R]->(y) RETURN y
        |}
        |WITH x, y RETURN x, y""".stripMargin
    )
  }

  test("WITH UNION WITH with same alias") {
    assertParity(
      """WITH 1 AS a RETURN a
        |UNION
        |WITH 2 AS a RETURN a""".stripMargin
    )
  }

  test("UNION of aliasing subqueries with order by inside each branch") {
    assertParity(
      """MATCH (n) WITH n AS x ORDER BY x RETURN x
        |UNION
        |MATCH (m) WITH m AS x ORDER BY x RETURN x""".stripMargin
    )
  }

  test("ORDER BY x after WITH that shadows outer x") {
    assertParity(
      """MATCH (a:A)
        |WITH a, a.num2 % 3 AS x
        |WITH a, a.num + a.num2 AS x
        |  ORDER BY x
        |  LIMIT 3
        |RETURN a, x""".stripMargin
    )
  }

  test("ORDER BY x in RETURN that shadows outer x") {
    assertParity(
      """MATCH (a:A)
        |WITH a, a.num2 % 3 AS x
        |RETURN a, a.num + a.num2 AS x
        |  ORDER BY x
        |  LIMIT 3""".stripMargin
    )
  }

  test("CALL (*) around chained UNION ALL with duplicate-alias branches") {
    assertParity(
      """WITH 1 AS a
        |CALL (*) {
        |  RETURN a AS b
        |  UNION ALL
        |  RETURN a + 1 AS b
        |  UNION ALL
        |  RETURN a + 2 AS b
        |  UNION ALL
        |  RETURN a + 2 AS b
        |}
        |RETURN a, b""".stripMargin
    )
  }

  test("DISTINCT WITH carrying constants through importing subquery") {
    assertParity(
      """WITH 1 AS x
        |CALL (*) {
        |  WITH DISTINCT *
        |  RETURN x AS y
        |}
        |RETURN x, y""".stripMargin
    )
  }

  test("second MATCH rebinds node variable from first MATCH") {
    assertParity(
      """MATCH (n)
        |MATCH (n)-[r]->(m)
        |RETURN n, r, m""".stripMargin
    )
  }

  test("ORDER BY alias equal to incoming variable") {
    assertParity(
      """MATCH (a)
        |WITH a
        |  ORDER BY a
        |RETURN a""".stripMargin
    )
  }

  test("NEXT statement reusing outer name") {
    assertParity(
      """MATCH (n)
        |RETURN n
        |NEXT
        |MATCH (n)
        |RETURN n""".stripMargin
    )
  }

  test("YIELD over two SHOW commands — command-column LHS must not be renamed") {
    assertParity(
      "SHOW DEFAULT DATABASE YIELD name AS b SHOW DATABASES YIELD name RETURN *"
    )
  }

  test("NESTED PATHS after ProjectNamedPaths — OLD vs NEW Namespacer") {
    val query =
      """MATCH p0 = ()
        |WITH DISTINCT p0
        |WHERE COUNT {
        |  WITH p0 AS a
        |  MATCH p1 = ()
        |  WITH DISTINCT p1
        |  WHERE COUNT { WITH p1 AS b } >= 0
        |  RETURN p1
        |} >= 0
        |RETURN p0""".stripMargin

    val version = org.neo4j.cypher.internal.CypherVersion.Cypher25

    def runPipelineUpToSecondNamespacer(): (BaseState, BaseContext) = {
      val prep = Namespacer andThen ProjectNamedPathsRewriter andThen ScopeSurveyor
      val state = prepareFrom(version, query, prep)
      NamespacerParityTestSupport.advanceCounterPastAnonNames(state)
      val ctx = ContextHelper.create(version, query, phaseTestConfig.semanticFeatures, databaseReference)
      (state, ctx)
    }

    val (oldState, ctxOld) = runPipelineUpToSecondNamespacer()
    val (newState, ctxNew) = runPipelineUpToSecondNamespacer()

    val oldResult = Namespacer.processOld(oldState, ctxOld)
    val newResult = Namespacer.processNew(newState, ctxNew)

    val oldPretty = prettifier.asString(NamespacerParityTestSupport.canonicalize(oldResult.statement()))
    val newPretty = prettifier.asString(NamespacerParityTestSupport.canonicalize(newResult.statement()))
    withClue(s"OLD:\n$oldPretty\n\nNEW:\n$newPretty\n") {
      newPretty shouldEqual oldPretty
    }
  }

  test("SHOW FUNCTIONS YIELD then RETURN ORDER BY name UNION with subquery RHS") {
    assertParity(
      """SHOW ALL FUNCTIONS
        |YIELD name
        |RETURN "function" AS type, name
        |  ORDER BY name ASCENDING
        |UNION
        |{
        |  OPTIONAL MATCH (n)
        |  RETURN "match" AS type, n.name AS name
        |}""".stripMargin
    )
  }

  test("list comprehension iterator shadows outer WITH binding used as grouping key") {
    assertParity(
      """WITH {name: 'a_name', prop: 'a_prop'} AS a,
        |     {name: 'b_name', prop: 'b_prop'} AS b
        |RETURN a.name, [a IN collect(b) | [a.name, a.prop]] AS r""".stripMargin
    )
  }

  test("reduce iterator shadows outer WITH binding used as grouping key") {
    assertParity(
      """WITH {name: 'a_name', prop: 'a_prop'} AS a,
        |     {name: 'b_name', prop: 'b_prop'} AS b
        |RETURN a.name, reduce(acc = '', a IN collect(b) | acc + a.name + ':' + a.prop) AS r""".stripMargin
    )
  }

  test("two co-travelling exports through three-level CALL nesting with WITH pass-throughs") {
    assertParity(
      """UNWIND [] AS x
        |WITH x, 1 AS t
        |CALL (x, t) {
        |  MERGE (a:A)
        |  MERGE (b:B)
        |  RETURN a, b
        |}
        |CALL (x, t, a, b) {
        |  WITH x, t, a, b WHERE x IS NOT NULL
        |  MATCH (m)
        |  CALL (m, t, a, b) {
        |    WITH m, t, a, b WHERE m IS NOT NULL
        |    MERGE (a)-[:R]->(m)
        |    MERGE (b)-[:R]->(m)
        |    WITH m, t, a, b WHERE m IS NOT NULL
        |    MERGE (e:E)
        |    MERGE (a)-[:S]->(e)
        |    MERGE (b)-[:S]->(e)
        |  }
        |}""".stripMargin
    )
  }

  test("QPP relationship-group variable and inner anonymous singletons keep distinct namespacing") {
    assertParity(
      """MATCH path = ()-[r]->{3,8}()
        |WHERE allReduce(acc = 0, rel IN r | acc + rel.q, -1 <= acc <= 1)
        |RETURN [rel IN r | rel.q] AS rels""".stripMargin
    )
  }

  test("allReduce iterator shadows outer QPP relationship-group variable") {
    assertParity(
      """MATCH path = ()-[r]->{3,8}()
        |WHERE allReduce(acc = 0, r IN r | acc + r.q, -1 <= acc <= 1)
        |RETURN [rel IN r | rel.q] AS rels""".stripMargin
    )
  }

  test("bounded variable-length paths with shared interior variable") {
    assertParity(
      """MATCH p = (n0:A)-->*(n1)-->*(n2:C), q = (n1)-->(n3:E)
        |RETURN nodes(p) AS path1, nodes(q) AS path2""".stripMargin
    )
  }

  test("FOREACH-local MERGE rebinds outer relationship variable as fresh edge") {
    assertParity(
      """MATCH ()-[r:R]->()
        |FOREACH (_ IN [1] | MERGE ()-[r:R {prop: 1}]->() )""".stripMargin
    )
  }

  test("FOREACH-local CREATE rebinds outer relationship variable as fresh edge") {
    assertParity(
      """MATCH ()-[r:R]->()
        |FOREACH (_ IN [1] | CREATE ()-[r:R]->() )""".stripMargin
    )
  }

  test("Nested FOREACH with CREATE rebinding outer relationship variable") {
    assertParity(
      """MATCH ()-[r:R]->()
        |FOREACH (_ IN [1] | FOREACH (_ IN [1] | CREATE ()-[r:R]->() ) )""".stripMargin
    )
  }

  test("CALL { WITH * } subquery containing FOREACH that rebinds outer relationship") {
    assertParity(
      """MATCH ()-[r:R]->()
        |CALL { WITH *
        |  FOREACH (_ IN [1] | MERGE ()-[r:R {prop: 1}]->() )
        |}
        |FINISH""".stripMargin
    )
  }

  test("CALL (*) subquery containing FOREACH that rebinds outer relationship") {
    assertParity(
      """MATCH ()-[r:R]->()
        |CALL (*) {
        |  FOREACH (_ IN [1] | CREATE ()-[r:R]->() )
        |}
        |FINISH""".stripMargin
    )
  }

  test("CASE discriminand reuses grouping-key property under aggregating UNION") {
    assertParity(
      """MATCH (n)
        |WITH n.k AS gk, CASE n.k WHEN 1 THEN COLLECT(n.v) ELSE COLLECT(n.v) END AS parts
        |RETURN gk, parts
        |UNION
        |MATCH (n) RETURN n.k AS gk, COLLECT(n.v) AS parts""".stripMargin
    )
  }

  test("list-comp grouping-key with iter shadowing outer name under UNION rename") {
    assertParity(
      """MATCH (a)
        |WITH [a IN [1,2,3] | a + 1] AS gk, count(*) AS c
        |RETURN gk, c
        |UNION
        |MATCH (a) RETURN [] AS gk, 1 AS c""".stripMargin
    )
  }

  test("reduce grouping-key with iter shadowing outer name under UNION rename") {
    assertParity(
      """MATCH (a)
        |WITH reduce(s = 0, a IN [1,2,3] | s + a) AS gk, count(*) AS c
        |RETURN gk, c
        |UNION
        |MATCH (a) RETURN 0 AS gk, 1 AS c""".stripMargin
    )
  }

  test("alias shadowing incoming variable used in ORDER BY (non-aggregating)") {
    assertParity(
      """MATCH (n)
        |WITH [n] AS n
        |  ORDER BY size(n), last(n).foo
        |RETURN n""".stripMargin
    )
  }

  test("alias shadowing incoming variable used in WHERE") {
    assertParity(
      """MATCH (n)
        |WITH [n] AS n
        |  WHERE size(n) > 0
        |RETURN n""".stripMargin
    )
  }

  test("two aliases each shadowing their own incoming variable used in ORDER BY") {
    assertParity(
      """MATCH (a), (b)
        |WITH [a] AS a, [b] AS b
        |  ORDER BY last(a), last(b).foo
        |RETURN a, b""".stripMargin
    )
  }

  test("aggregating alias shadowing incoming used in ORDER BY") {
    assertParity(
      """MATCH (n)
        |WITH collect(n) AS n
        |  ORDER BY size(n)
        |RETURN n""".stripMargin
    )
  }
}
