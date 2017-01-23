/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.helpers.StatementHelper._
import org.neo4j.cypher.internal.compiler.v3_0.parser.ParserFixture.parser
import org.neo4j.cypher.internal.compiler.v3_0.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast.{ASTAnnotationMap, AstConstructionTestSupport, Statement, Variable}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport {

  val tests = Seq(
    "match (n) return n as n" ->
    "match (n) return n as n"
    ,
    "match (n), (x) with n as n match (x) return n as n, x as x" ->
    "match (n), (`  x@12`) with n as n match (`  x@34`) return n as n, `  x@34` as x"
    ,
    "match (n), (x) where [x in n.prop where x = 2] return x as x" ->
    "match (n), (`  x@12`) where [`  x@22` in n.prop where `  x@22` = 2] return `  x@12` as x"
    ,
    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *" ->
    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *"
    ,
    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll" ->
    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll"
    ,
    "match (me)-[r1]->(you) with 1 AS x match (me)-[r1]->(food)<-[r2]-(you) return r1.times as `r1.times`" ->
    "match (`  me@7`)-[`  r1@12`]->(`  you@18`) with 1 AS x match (`  me@42`)-[`  r1@47`]->(food)<-[r2]-(`  you@66`) return `  r1@47`.times as `r1.times`"
    ,
    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *" ->
    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *"
    ,
    "match (a:Party) return a as a union match (a:Animal) return a as a" ->
    "match (`  a@7`:Party) return `  a@7` as a union match (`  a@43`:Animal) return `  a@43` as a"
    ,
    "match p=(a:Start)-->(b) return *" ->
    "match p=(a:Start)-->(b) return *"
    ,
    "match (n) return n, count(*) as c order by c" ->
    """match (`  n@7`)
      |with `  n@7` as `  FRESHID17`, count(*) as `  FRESHID20` ORDER BY `  FRESHID20`
      |return `  FRESHID17` as n, `  FRESHID20` as c""".stripMargin
    ,
    "START root=node:Person(id='deevian') RETURN id(root) as id UNION START root=node:Person(id='retophy') RETURN id(root) as id" ->
    "START `  root@6`=node:Person(id='deevian') RETURN id(`  root@6`) as id UNION START `  root@71`=node:Person(id='retophy') RETURN id(`  root@71`) as id"
    ,
    "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng" ->
    "WITH 1 AS `  p@10`, count(*) AS rng WITH `  p@10`  AS `  FRESHID36` ORDER BY rng RETURN `  FRESHID36` AS p",
    "CALL db.labels() YIELD label WITH count(*) AS c CALL db.labels() YIELD label RETURN *" ->
    "CALL db.labels() YIELD label AS `  label@23` WITH count(*) AS c CALL db.labels() YIELD label AS `  label@71` RETURN c AS c, `  label@71` AS label"
  )

  tests.foreach {
    case (q, rewritten) =>
      test(q) {
        assertRewritten(q, rewritten)
      }
  }

  test("Renames variables in semantic table") {
    val idA1 = Variable("a")(InputPosition(1, 0, 1))
    val idA2 = Variable("a")(InputPosition(2, 0, 2))
    val idA3 = Variable("a")(InputPosition(3, 0, 3))
    val idB5 = Variable("b")(InputPosition(5, 0, 5))

    val infoA1 = mock[ExpressionTypeInfo]
    val infoA2 = mock[ExpressionTypeInfo]
    val infoA3 = mock[ExpressionTypeInfo]
    val infoA4 = mock[ExpressionTypeInfo]
    val infoB5 = mock[ExpressionTypeInfo]

    val table = SemanticTable(ASTAnnotationMap(
      idA1 -> infoA1,
      idA2 -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))

    val renamings = Map(
      Ref(idA1) -> Variable("a@1")(InputPosition(1, 0, 1)),
      Ref(idA2) -> Variable("a@2")(InputPosition(2, 0, 2))

    )

    val namespacer = Namespacer(renamings)

    val newTable = namespacer.tableRewriter(table)

    newTable.types should equal(ASTAnnotationMap(
      Variable("a@1")(InputPosition(1, 0, 1)) -> infoA1,
      Variable("a@2")(InputPosition(2, 0, 2)) -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))
  }

  val astRewriter = new ASTRewriter(RewriterStepSequencer.newValidating, false)

  private def assertRewritten(from: String, to: String) = {
    val fromAst = parseAndRewrite(from)
    val state = fromAst.semanticState
    val namespacer = Namespacer(fromAst, state.scopeTree)
    val namespacedAst = fromAst.endoRewrite(namespacer.statementRewriter)

    val expectedAst = parseAndRewrite(to)

    namespacedAst should equal(expectedAst)
  }

  private def parseAndRewrite(queryText: String): Statement = {
    val parsedAst = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val cleanedAst = parsedAst.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val (rewrittenAst, _, _) = astRewriter.rewrite(queryText, cleanedAst, cleanedAst.semanticState)
    rewrittenAst
  }
}
