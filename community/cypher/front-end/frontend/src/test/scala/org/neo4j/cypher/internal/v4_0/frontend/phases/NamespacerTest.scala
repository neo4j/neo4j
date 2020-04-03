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
package org.neo4j.cypher.internal.v4_0.frontend.phases

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.v4_0.ast.Query
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.Union.UnionMapping
import org.neo4j.cypher.internal.v4_0.ast.Where
import org.neo4j.cypher.internal.v4_0.expressions.ExistsSubClause
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.expressions.HasLabels
import org.neo4j.cypher.internal.v4_0.expressions.LabelName
import org.neo4j.cypher.internal.v4_0.expressions.NodePattern
import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  private val tests: Seq[Test] = Seq(
    TestCase(
      "MATCH (n) RETURN n as n",
      "MATCH (n) RETURN n as n",
      List.empty
    ),
    TestCase(
      "MATCH (n), (x) WITH n AS n MATCH (x) RETURN n AS n, x AS x",
      "MATCH (n), (`  x@12`) WITH n AS n MATCH (`  x@34`) RETURN n AS n, `  x@34` AS `  x@34`",
      List(varFor("  x@12"), varFor("  x@34"))
    ),
    TestCase(
      "MATCH (n), (x) WHERE [x in n.prop WHERE x = 2] RETURN x AS x",
      "MATCH (n), (`  x@12`) WHERE [`  x@22` IN n.prop WHERE `  x@22` = 2] RETURN `  x@12` AS `  x@12`",
      List(
        varFor("  x@12"),
        varFor("  x@22"),
        equals(varFor("  x@22"), literalInt(2)),
        listComprehension(
          varFor("  x@22"),
          prop("n", "prop"),
          Some(equals(varFor("  x@22"), literalInt(2))),
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
      "MATCH (`  me@7`)-[`  r1@12`]->(`  you@18`) WITH 1 AS x MATCH (`  me@42`)-[`  r1@47`]->(food)<-[r2]-(`  " +
        "you@66`) " +
        "RETURN `  r1@47`.times AS `r1.times`",
      List(
        varFor("  me@7"),
        varFor("  r1@12"),
        varFor("  you@18"),
        varFor("  me@42"),
        varFor("  r1@47"),
        varFor("  you@66")
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
          match_(NodePattern(Some(varFor("  a@7")), Seq.empty, None)(pos), Some(Where(HasLabels(varFor("  a@7"), Seq(LabelName("Party")(pos)))(pos))(pos))),
          return_(varFor("  a@7").as("  a@7"))
        ),
        singleQuery(
          match_(NodePattern(Some(varFor("  a@43")), Seq.empty, None)(pos), Some(Where(HasLabels(varFor("  a@43"), Seq(LabelName("Animal")(pos)))(pos))(pos))),
          return_(varFor("  a@43").as("  a@43"))
        ),
        List(UnionMapping(varFor("  a@30"), varFor("  a@7"), varFor("  a@43"))))(pos))(pos),
      List(varFor("  a@7"), varFor("  a@43"))
    ),
    TestCase(
      "MATCH p=(a:Start)-->(b) RETURN *",
      "MATCH p=(a:Start)-->(b) RETURN *",
      List.empty
    ),
    TestCase(
      "MATCH (n) RETURN n, count(*) AS c order by c",
      """MATCH (`  n@7`)
        |RETURN `  n@7` AS `  n@18`, count(*) AS c ORDER BY c""".stripMargin,
      List(varFor("  n@7"))
    ),
    TestCase(
      "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      List(varFor("p"))
    ),
    TestCase(
      "CALL db.labels() YIELD label WITH count(*) AS c CALL db.labels() YIELD label RETURN *",
      "CALL db.labels() YIELD label AS `  label@23` WITH count(*) AS c CALL db.labels() YIELD label AS `  label@71` RETURN c AS c, `  label@71` AS `  label@71`",
      List(varFor("  label@23"), varFor("  label@71"))
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
      """MATCH (`  video@7`)
        |WITH {key:`  video@7`} AS `  video@34`
        |RETURN `  video@34`.key AS x""".stripMargin,
      List(
        varFor("  video@7"),
        varFor("  video@34"),
        prop("  video@34", "key")
      )
    ),
    TestCase(
      """WITH [1,2,3] AS nums
        |RETURN reduce(weight=0, num IN nums | weight + num) AS weight
        |ORDER BY weight DESC""".stripMargin,
      """WITH [1,2,3] AS nums
        |RETURN reduce(`  weight@35`=0, num IN nums | `  weight@35` + num ) AS `  weight@76`
        |ORDER BY `  weight@76` DESC""".stripMargin,
      List(varFor("  weight@35"), varFor("  weight@76"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (foo IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY foo",
      "WITH 1 AS `  foo@10` FOREACH (`  foo@23` IN [1,2,3] | CREATE (c)) RETURN `  foo@10` as bar ORDER BY `  foo@10`",
      List(varFor("  foo@10"), varFor("  foo@23"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (bar IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY bar",
      "WITH 1 AS foo FOREACH (`  bar@23` IN [1,2,3] | CREATE (c)) RETURN foo as `  bar@66` ORDER BY `  bar@66`",
      List(varFor("  bar@23"), varFor("  bar@66"))
    ),
    TestCase(
      "WITH 1 AS foo FOREACH (bar IN [1,2,3] | CREATE (c)) RETURN foo as bar ORDER BY bar + 2",
      "WITH 1 AS foo FOREACH (`  bar@23` IN [1,2,3] | CREATE (c)) RETURN foo as `  bar@66` ORDER BY `  bar@66` + 2",
      List(varFor("  bar@23"), varFor("  bar@66"))
    ),
    TestCase(
      "MATCH (a) WITH a.name AS n ORDER BY a.foo MATCH (a) RETURN a.age",
      "MATCH (`  a@7`) WITH `  a@7`.name as n ORDER BY `  a@7`.foo MATCH (`  a@49`) RETURN `  a@49`.age AS `a.age`",
      List(varFor("  a@7"), varFor("  a@49"))
    ),
    TestCase(
      "MATCH (n) WHERE EXISTS { MATCH (n)-->(p) } WITH n as m, 1 as n RETURN m, n",
      "MATCH (`  n@7`) WHERE EXISTS { MATCH (`  n@7`)-->(p) } WITH `  n@7` as m, 1 as `  n@61` RETURN m, `  n@61`",
      List(varFor("  n@7"), varFor("  n@61"))
    )
  )

  test("should rewrite outer scope variables of exists subclause even if not used in subclause") {
    val query = "MATCH (n) WHERE EXISTS { MATCH (p:Label) } WITH n as m, 1 as n RETURN m, n"

    val statement = prepareFrom(query).statement()

    val outerScope = statement.treeFold(Set.empty[Variable]) {
      case expr: ExistsSubClause =>
        acc => (acc ++ expr.outerScope, Some(identity))
    }

    outerScope.map(_.name) should be(Set("  n@7"))
  }

  override def rewriterPhaseUnderTest: Phase[BaseContext, BaseState, BaseState] = Namespacer

  sealed trait Test

  case class TestCase(query: String, rewrittenQuery: String, semanticTableExpressions: List[Expression]) extends Test
  case class TestCaseWithStatement(query: String, rewrittenQuery: Statement, semanticTableExpressions: List[Expression]) extends Test

  tests.foreach {
    case TestCase(q, rewritten, semanticTableExpressions) =>
      test(q) {
        assertRewritten(q, rewritten, semanticTableExpressions)
      }
    case TestCaseWithStatement(q, rewritten, semanticTableExpressions) =>
      test(q) {
        assertRewritten(q, rewritten, semanticTableExpressions)
      }
  }
}


