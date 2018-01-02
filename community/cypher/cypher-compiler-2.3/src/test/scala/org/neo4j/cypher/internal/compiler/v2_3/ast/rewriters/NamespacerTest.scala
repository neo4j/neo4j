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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.StatementHelper._
import org.neo4j.cypher.internal.compiler.v2_3.parser.ParserFixture.parser
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{ASTAnnotationMap, AstConstructionTestSupport, Identifier, Statement}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NamespacerTest extends CypherFunSuite with AstConstructionTestSupport {

  val tests = Seq(
    "match n return n as n" ->
    "match n return n as n"
    ,
    "match n, x with n as n match x return n as n, x as x" ->
    "match n, `  x@9` with n as n match `  x@29` return n as n, `  x@29` as x"
    ,
    "match n, x where [x in n.prop where x = 2] return x as x" ->
    "match n, `  x@9` where [`  x@18` in n.prop where `  x@18` = 2] return `  x@9` as x"
    ,
    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *" ->
    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *"
    ,
    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll" ->
    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll"
    ,
    "match me-[r1]->you with 1 AS x match me-[r1]->food<-[r2]-you return r1.times as `r1.times`" ->
    "match `  me@6`-[`  r1@10`]->`  you@15` with 1 AS x match `  me@37`-[`  r1@41`]->food<-[r2]-`  you@57` return `  r1@41`.times as `r1.times`"
    ,
    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *" ->
    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *"
    ,
    "match (a:Party) return a as a union match (a:Animal) return a as a" ->
    "match (`  a@7`:Party) return `  a@7` as a union match (`  a@43`:Animal) return `  a@43` as a"
    ,
    "match p=(a:Start)-->b return *" ->
    "match p=(a:Start)-->b return *"
    ,
    "match n return n, count(*) as c order by c" ->
    """match `  n@6`
      |with `  n@6` as `  FRESHID15`, count(*) as `  FRESHID18` ORDER BY `  FRESHID18`
      |return `  FRESHID15` as n, `  FRESHID18` as c""".stripMargin
    ,
    "START root=node:Person(id='deevian') RETURN id(root) as id UNION START root=node:Person(id='retophy') RETURN id(root) as id" ->
    "START `  root@6`=node:Person(id='deevian') RETURN id(`  root@6`) as id UNION START `  root@71`=node:Person(id='retophy') RETURN id(`  root@71`) as id"
    ,
    "WITH 1 AS p, count(*) AS rng RETURN p ORDER BY rng" ->
    "WITH 1 AS `  p@10`, count(*) AS rng WITH `  p@10`  AS `  FRESHID36` ORDER BY rng RETURN `  FRESHID36` AS p"
  )

  tests.foreach {
    case (q, rewritten) =>
      test(q) {
        assertRewritten(q, rewritten)
      }
  }

  test("Renames identifiers in semantic table") {
    val idA1 = Identifier("a")(InputPosition(1, 0, 1))
    val idA2 = Identifier("a")(InputPosition(2, 0, 2))
    val idA3 = Identifier("a")(InputPosition(3, 0, 3))
    val idB5 = Identifier("b")(InputPosition(5, 0, 5))

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
      Ref(idA1) -> Identifier("a@1")(InputPosition(1, 0, 1)),
      Ref(idA2) -> Identifier("a@2")(InputPosition(2, 0, 2))

    )

    val namespacer = Namespacer(renamings)

    val newTable = namespacer.tableRewriter(table)

    newTable.types should equal(ASTAnnotationMap(
      Identifier("a@1")(InputPosition(1, 0, 1)) -> infoA1,
      Identifier("a@2")(InputPosition(2, 0, 2)) -> infoA2,
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
