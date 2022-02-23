/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  private val tests: Seq[Test] = Seq(
    TestCase(
      "MATCH (n) RETURN n as n",
      "MATCH (n) RETURN n as n",
      List.empty
    ),
    TestCase(
      "MATCH (n), (x) WITH n AS n MATCH (x) RETURN n AS n, x AS x",
      "MATCH (n), (`  x@0`) WITH n AS n MATCH (`  x@1`) RETURN n AS n, `  x@1` AS `  x@1`",
      List(varFor("  x@0"), varFor("  x@1"))
    ),
    TestCase(
      "MATCH (n), (x) WHERE [x in n.prop WHERE x = 2] RETURN x AS x",
      "MATCH (n), (`  x@0`) WHERE [`  x@1` IN n.prop WHERE `  x@1` = 2] RETURN `  x@0` AS `  x@0`",
      List(
        varFor("  x@0"),
        varFor("  x@1"),
        equals(varFor("  x@1"), literalInt(2)),
        listComprehension(
          varFor("  x@1"),
          prop("n", "prop"),
          Some(equals(varFor("  x@1"), literalInt(2))),
          None
        )
      )
    ),
    TestCase(
      "MATCH (a) WITH a.bar AS bars WHERE 1 = 2 RETURN *",
      "MATCH (a) WITH a.bar AS bars WHERE 1 = 2 RETURN *",
      List.empty
    ),
    TestCase(
      "MATCH (n) WHERE id(n) = 0 WITH collect(n) AS coll WHERE size(coll)=$id RETURN coll",
      "MATCH (n) WHERE id(n) = 0 WITH collect(n) AS coll WHERE size(coll)=$id RETURN coll",
      List.empty
    ),
    TestCase(
      "MATCH (me)-[r1]->(you) WITH 1 AS x MATCH (me)-[r1]->(food)<-[r2]-(you) RETURN r1.times AS `r1.times`",
      "MATCH (`  me@0`)-[`  r1@1`]->(`  you@2`) WITH 1 AS x MATCH (`  me@3`)-[`  r1@4`]->(food)<-[r2]-(`  you@5`) " +
        "RETURN `  r1@4`.times AS `r1.times`",
      List(
        varFor("  me@0"),
        varFor("  r1@1"),
        varFor("  you@2"),
        varFor("  me@3"),
        varFor("  r1@4"),
        varFor("  you@5")
      )
    ),
    TestCase(
      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *",
      "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *",
      List.empty
    ),
    TestCaseWithStatement(
      "MATCH (a:Party) RETURN a AS a UNION MATCH (a:Animal) RETURN a AS a",
      Query(None, ProjectingUnionDistinct(
        singleQuery(
          match_(NodePattern(Some(varFor("  a@0")), Seq.empty, None, None)(pos), Some(Where(HasLabels(varFor("  a@0"), Seq(LabelName("Party")(pos)))(pos))(pos))),
          return_(varFor("  a@0").as("  a@0"))
        ),
        singleQuery(
          match_(NodePattern(Some(varFor("  a@1")), Seq.empty, None, None)(pos), Some(Where(HasLabels(varFor("  a@1"), Seq(LabelName("Animal")(pos)))(pos))(pos))),
          return_(varFor("  a@1").as("  a@1"))
        ),
        List(UnionMapping(varFor("  a@2"), varFor("  a@0"), varFor("  a@1"))))(pos))(pos),
      List(varFor("  a@0"), varFor("  a@1"))
    ),
    TestCase(
      "MATCH p=(a:Start)-[r]->(b) RETURN *",
      "MATCH p=(a:Start)-[r]->(b) RETURN *",
      List.empty
    ),
    TestCase(
      "MATCH (n) RETURN n AS n, count(*) AS c order by c",
      """MATCH (`  n@0`)
        |RETURN `  n@0` AS `  n@1`, count(*) AS c ORDER BY c""".stripMargin,
      List(varFor("  n@0"))
    ),
    TestCase(
      "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      List(varFor("p"))
    ),
    TestCase(
      "CALL db.labels() YIELD label WITH count(*) AS c CALL db.labels() YIELD label RETURN *",
      "CALL db.labels() YIELD label AS `  label@0` WITH count(*) AS c CALL db.labels() YIELD label AS `  label@1` RETURN c AS c, `  label@1` AS `  label@1`",
      List(varFor("  label@0"), varFor("  label@1"))
    ),
    TestCase(
      "MATCH (a),(b) WITH a AS a, a.prop AS AG1, collect(b.prop) AS AG2 RETURN a{prop: AG1, k: AG2} AS X",
      "MATCH (a),(b) WITH a AS a, a.prop AS AG1, collect(b.prop) AS AG2 RETURN a{prop: AG1, k: AG2} AS X",
      List.empty
    ),
    TestCase(
      """MATCH (video)
        |WITH {key:video} AS video
        |RETURN video.key AS x""".stripMargin,
      """MATCH (`  video@0`)
        |WITH {key:`  video@0`} AS `  video@1`
        |RETURN `  video@1`.key AS x""".stripMargin,
      List(
        varFor("  video@0"),
        varFor("  video@1"),
        prop("  video@1", "key")
      )
    ),
    TestCase(
      """WITH [1,2,3] AS nums
        |RETURN reduce(weight=0, num IN nums | weight + num) AS weight
        |ORDER BY weight DESC""".stripMargin,
      """WITH [1,2,3] AS nums
        |RETURN reduce(`  weight@0`=0, num IN nums | `  weight@0` + num ) AS `  weight@1`
        |ORDER BY `  weight@1` DESC""".stripMargin,
      List(varFor("  weight@0"), varFor("  weight@1"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (foo IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY foo",
      "WITH 1 AS `  foo@0` FOREACH (`  foo@1` IN [1,2,3] | CREATE (c)) RETURN `  foo@0` as bar ORDER BY `  foo@0`",
      List(varFor("  foo@0"), varFor("  foo@1"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (bar IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY bar",
      "WITH 1 AS foo FOREACH (`  bar@0` IN [1,2,3] | CREATE (c)) RETURN foo as `  bar@1` ORDER BY `  bar@1`",
      List(varFor("  bar@0"), varFor("  bar@1"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (bar IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY bar + 2",
      "WITH 1 AS foo FOREACH (`  bar@0` IN [1,2,3] | CREATE (c)) RETURN foo as `  bar@1` ORDER BY `  bar@1` + 2",
      List(varFor("  bar@0"), varFor("  bar@1"))
    ),
    TestCase(
      "MATCH (a) WITH a.name AS n ORDER BY a.foo MATCH (a) RETURN a.age",
      "MATCH (`  a@0`) WITH `  a@0`.name as n ORDER BY `  a@0`.foo MATCH (`  a@1`) RETURN `  a@1`.age AS `a.age`",
      List(varFor("  a@0"), varFor("  a@1"))
    ),
    TestCase(
      "MATCH (n) WHERE EXISTS { MATCH (n)-[r]->(p) } WITH n as m, 1 as n RETURN m, n",
      "MATCH (`  n@0`) WHERE EXISTS { MATCH (`  n@0`)-[r]->(p) } WITH `  n@0` as m, 1 as `  n@1` RETURN m, `  n@1`",
      List(varFor("  n@0"), varFor("  n@1"))
    )
  )

  test("should rewrite outer scope variables of exists subclause even if not used in subclause") {
    val query = "MATCH (n) WHERE EXISTS { MATCH (p:Label) } WITH n as m, 1 as n RETURN m, n"

    val statement = prepareFrom(query, rewriterPhaseUnderTest).statement()

    val outerScope = statement.folder.treeFold(Set.empty[LogicalVariable]) {
      case expr: ExistsSubClause =>
        acc => TraverseChildren(acc ++ expr.outerScope)
    }

    outerScope.map(_.name) should be(Set("  n@0"))
  }

  //noinspection ZeroIndexToHead
  test("should disambiguate anonymous names with new anonymous names") {
    val namer = new AnonymousVariableNameGenerator()
    val names = Seq(namer.nextName, namer.nextName, namer.nextName).map(s => s"`$s`")

    val query = s"UNWIND [1,2,3] AS ${names(0)} WITH ${names(0)} + 1 AS x MATCH (${names(0)})-[${names(1)}]-(${names(2)}) RETURN 1 AS foo"
    val statement = prepareFrom(query, rewriterPhaseUnderTest).statement()

    statement.folder.findAllByClass[Variable].map(_.name).foreach {
      case "x" | "foo" => // OK
      case v if AnonymousVariableNameGenerator.notNamed(v) => // OK
      case v => fail(s"$v was not an anonymous variable")
    }
  }

  override def rewriterPhaseUnderTest: Phase[BaseContext, BaseState, BaseState] = Namespacer

  sealed trait Test

  case class TestCase(query: String, rewrittenQuery: String, semanticTableExpressions: List[Expression]) extends Test
  case class TestCaseWithStatement(query: String, rewrittenQuery: Statement, semanticTableExpressions: List[Expression]) extends Test

  tests.foreach {
    case TestCase(q, rewritten, semanticTableExpressions) =>
      test(q) {
        assertRewritten(q.replace("\r\n", "\n"), rewritten, semanticTableExpressions)
      }
    case TestCaseWithStatement(q, rewritten, semanticTableExpressions) =>
      test(q) {
        assertRewritten(q.replace("\r\n", "\n"), rewritten, semanticTableExpressions)
      }
  }
}
