/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.parser.ParserFixture.parser
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Statement}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.StatementHelper._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticFeature
import org.neo4j.cypher.internal.planner.v3_4.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.v3_4.inSequence
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport with LogicalPlanConstructionTestSupport {

  case class TestCase(query: String, rewrittenQuery: String, semanticTableExpressions: List[Expression])

  val tests = Seq(
    TestCase(
      "MATCH (n) RETURN n as n",
      "MATCH (n) RETURN n as n",
      List.empty
    ),
    TestCase(
      "MATCH (n), (x) WITH n AS n MATCH (x) RETURN n AS n, x AS x",
      "MATCH (n), (`  x@12`) WITH n AS n MATCH (`  x@34`) RETURN n AS n, `  x@34` AS x",
      List(varFor("  x@12"), varFor("  x@34"))
    ),
    TestCase(
      "MATCH (n), (x) WHERE [x in n.prop WHERE x = 2] RETURN x AS x",
      "MATCH (n), (`  x@12`) WHERE [`  x@22` IN n.prop WHERE `  x@22` = 2] RETURN `  x@12` AS x",
      List(
        varFor("  x@12"),
        varFor("  x@22"),
        Equals(varFor("  x@22"), SignedDecimalIntegerLiteral("2")(pos))(pos),
        ListComprehension(
          ExtractScope(
            varFor("  x@22"),
            Some(Equals(varFor("  x@22"), SignedDecimalIntegerLiteral("2")(pos))(pos)),
            None
          )(pos),
          Property(varFor("n"), PropertyKeyName("prop")(pos))(pos)
        )(pos)
      )
    ),
    TestCase(
      "MATCH (a) WITH a.bar AS bars WHERE 1 = 2 RETURN *",
      "MATCH (a) WITH a.bar AS bars WHERE 1 = 2 RETURN *",
      List.empty
    ),
    TestCase(
      "MATCH (n) WHERE id(n) = 0 WITH collect(n) AS coll WHERE length(coll)={id} RETURN coll",
      "MATCH (n) WHERE id(n) = 0 WITH collect(n) AS coll WHERE length(coll)={id} RETURN coll",
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
    TestCase(
      "MATCH (a:Party) RETURN a AS a union MATCH (a:Animal) RETURN a AS a",
      "MATCH (`  a@7`:Party) RETURN `  a@7` AS a union MATCH (`  a@43`:Animal) RETURN `  a@43` AS a",
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
        |WITH `  n@7` AS `  FRESHID17`, count(*) AS `  FRESHID20` ORDER BY `  FRESHID20`
        |RETURN `  FRESHID17` AS n, `  FRESHID20` AS c""".stripMargin,
      List(varFor("  n@7"))
    ),
    TestCase(
      "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      "WITH 1 AS `  p@10`, count(*) AS rng WITH `  p@10`  AS `  FRESHID36` ORDER BY rng RETURN `  FRESHID36` AS p",
      List(varFor("  p@10"))
    ),
    TestCase(
      "CALL db.labels() YIELD label WITH count(*) AS c CALL db.labels() YIELD label RETURN *",
      "CALL db.labels() YIELD label AS `  label@23` WITH count(*) AS c CALL db.labels() YIELD label AS `  label@71` RETURN c AS c, `  label@71` AS label",
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
        Property(varFor("  video@34"), PropertyKeyName("key")(pos))(pos)
      )
    )
  )

  tests.foreach {
    case TestCase(q, rewritten, semanticTableExpressions) =>
      test(q) {
        assertRewritten(q, rewritten, semanticTableExpressions)
      }
  }

  val astRewriter = new ASTRewriter(RewriterStepSequencer.newValidating, Never, getDegreeRewriting = true)

  private def assertRewritten(from: String, to: String, semanticTableExpressions: List[Expression], features: SemanticFeature*): Unit = {
    val fromAst = parseAndRewrite(from, features: _*)
    val fromState = LogicalPlanState(from, None, IDPPlannerName, new StubSolveds, new StubCardinalities, Some(fromAst), Some(fromAst.semanticState(features: _*)))
    val toState = Namespacer.transform(fromState, ContextHelper.create())

    val expectedAst = parseAndRewrite(to, features: _*)

    toState.statement should equal(expectedAst)
    semanticTableExpressions.foreach { e =>
      toState.semanticTable().types.keys should contain(e)
    }
  }

  private def parseAndRewrite(queryText: String, features: SemanticFeature*): Statement = {
    val parsedAst = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val cleanedAst = parsedAst.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val (rewrittenAst, _, _) = astRewriter.rewrite(queryText, cleanedAst, cleanedAst.semanticState(features: _*))
    rewrittenAst
  }
}
