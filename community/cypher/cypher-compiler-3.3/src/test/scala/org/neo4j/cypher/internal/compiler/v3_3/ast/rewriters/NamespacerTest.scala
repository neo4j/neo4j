/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.parser.ParserFixture.parser
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.StatementHelper._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport {

  val tests = Seq(
    ("match (n) return n as n",
      ("match (n) return n as n", List.empty))
    ,
    ("match (n), (x) with n as n match (x) return n as n, x as x",
      ("match (n), (`  x@12`) with n as n match (`  x@34`) return n as n, `  x@34` as x", List(varFor("  x@12"), varFor("  x@34"))))
    ,
    ("match (n), (x) where [x in n.prop where x = 2] return x as x",
      ("match (n), (`  x@12`) where [`  x@22` in n.prop where `  x@22` = 2] return `  x@12` as x",
        List(varFor("  x@12"), varFor("  x@22"), Equals(varFor("  x@22"), SignedDecimalIntegerLiteral("2")(pos))(pos),
          ListComprehension(ExtractScope(varFor("  x@22"),Some(Equals(varFor("  x@22"),SignedDecimalIntegerLiteral("2")(pos))(pos)),None)(pos),
            Property(varFor("n"),PropertyKeyName("prop")(pos))(pos))(pos))))
    ,
    ("MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *",
      ("MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *", List.empty))
    ,
    ("match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll",
      ("match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll", List.empty))
    ,
    ("match (me)-[r1]->(you) with 1 AS x match (me)-[r1]->(food)<-[r2]-(you) return r1.times as `r1.times`",
      ("match (`  me@7`)-[`  r1@12`]->(`  you@18`) with 1 AS x match (`  me@42`)-[`  r1@47`]->(food)<-[r2]-(`  you@66`) return `  r1@47`.times as `r1.times`",
        List(varFor("  me@7"), varFor("  r1@12"), varFor("  you@18"), varFor("  me@42"), varFor("  r1@47"), varFor("  you@66"))))
    ,
    ("MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *",
      ("MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *", List.empty))
    ,
    ("match (a:Party) return a as a union match (a:Animal) return a as a",
      ("match (`  a@7`:Party) return `  a@7` as a union match (`  a@43`:Animal) return `  a@43` as a", List(varFor("  a@7"), varFor("  a@43"))))
    ,
    ("match p=(a:Start)-->(b) return *",
      ("match p=(a:Start)-->(b) return *", List.empty))
    ,
    ("match (n) return n, count(*) as c order by c",
      ("""match (`  n@7`)
      |with `  n@7` as `  FRESHID17`, count(*) as `  FRESHID20` ORDER BY `  FRESHID20`
      |return `  FRESHID17` as n, `  FRESHID20` as c""".stripMargin,
        List(varFor("  n@7"))))
    ,
    ("WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng",
      ("WITH 1 AS `  p@10`, count(*) AS rng WITH `  p@10`  AS `  FRESHID36` ORDER BY rng RETURN `  FRESHID36` AS p", List(varFor("  p@10"))))
    ,
    ("CALL db.labels() YIELD label WITH count(*) AS c CALL db.labels() YIELD label RETURN *",
      ("CALL db.labels() YIELD label AS `  label@23` WITH count(*) AS c CALL db.labels() YIELD label AS `  label@71` RETURN c AS c, `  label@71` AS label",
        List(varFor("  label@23"), varFor("  label@71"))))
    ,
    ("MATCH (a),(b) WITH a as a, a.prop as AG1, collect(b.prop) as AG2 RETURN a{prop: AG1, k: AG2} as X",
      ("MATCH (a),(b) WITH a as a, a.prop as AG1, collect(b.prop) as AG2 RETURN a{prop: AG1, k: AG2} as X", List.empty))
    ,

    (""" |MATCH (video)
        |WITH {key:video} AS video
        |RETURN video.key AS x""".stripMargin,
      ("""|MATCH (`  video@7`)
          |WITH {key:`  video@7`} AS `  video@34`
          |RETURN `  video@34`.key AS x""".stripMargin,
        List(varFor("  video@7"), varFor("  video@34"), Property(varFor("  video@34"), PropertyKeyName("key")(pos))(pos))))
  )

  tests.foreach {
    case (q, (rewritten, semanticTableExpressions)) =>
      test(q) {
        assertRewritten(q, rewritten, semanticTableExpressions)
      }
  }

  test("graph return items in RETURN are protected") {
    val original =
      """FROM GRAPH AT '/test/graph2' AS myGraph
        |MATCH (n:Person)
        |RETURN n.name AS name GRAPHS myGraph
      """.stripMargin

    assertRewritten(original, original, List.empty, SemanticFeature.MultipleGraphs)
  }

  test("graph return items in WITH are protected") {
    val original =
      """FROM GRAPH AT '/test/graph2' AS myGraph
        |WITH 1 AS a GRAPHS myGraph
        |MATCH (n:Person)
        |WITH n GRAPHS GRAPH AT 'foo' AS fooG >> myGraph AS barG
        |RETURN n.name AS name GRAPHS fooG, barG
      """.stripMargin

    assertRewritten(original, original, List.empty, SemanticFeature.MultipleGraphs)
  }

  val astRewriter = new ASTRewriter(RewriterStepSequencer.newValidating, Never)

  private def assertRewritten(from: String, to: String, semanticTableExpressions: List[Expression], features: SemanticFeature*): Unit = {
    val fromAst = parseAndRewrite(from, features: _*)
    val fromState = LogicalPlanState(from, None, IDPPlannerName, Some(fromAst), Some(fromAst.semanticState(features: _*)))
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
    val cleanedAst = parsedAst.endoRewrite(inSequence(normalizeGraphReturnItems, normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val (rewrittenAst, _, _) = astRewriter.rewrite(queryText, cleanedAst, cleanedAst.semanticState(features: _*))
    rewrittenAst
  }
}
