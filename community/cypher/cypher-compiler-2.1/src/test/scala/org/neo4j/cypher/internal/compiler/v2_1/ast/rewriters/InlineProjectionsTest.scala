/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp

class InlineProjectionsTest extends CypherFunSuite with AstRewritingTestSupport {
  // TODO: multiple withes + shadowing
  // TODO: local scope (extract, ...)

  test("should  inline: MATCH n WITH n AS m RETURN m => MATCH n RETURN n") {
    val result = projectionInlinedAst("MATCH n WITH n AS m RETURN m")

    result should equal(ast("MATCH n WITH * RETURN n AS m"))
  }

  test("should inline: WITH 1 AS x RETURN 1 + x => WITH * RETURN 1 + 1") {
    val result = projectionInlinedAst("WITH 1 AS x RETURN 1 + x")

    result should equal(ast("WITH * RETURN 1 + 1 AS `1 + x`"))
  }

  test("should inline: WITH 1 as b RETURN b => RETURN 1 AS `b`") {
    val result = projectionInlinedAst(" WITH 1 as b RETURN b")

    result should equal(ast("WITH * RETURN 1 AS `b`"))
  }

  test("should not inline aggregations: WITH 1 as b WITH DISTINCT b AS c RETURN c => WITH DISTINCT 1 AS c RETURN c AS c") {
    val result = projectionInlinedAst("WITH 1 as b WITH DISTINCT b AS c RETURN c")

    result should equal(ast("WITH * WITH DISTINCT 1 AS `c` RETURN c AS `c`"))
  }

  test("should not inline identifiers into patterns: WITH 1 as a MATCH (a) RETURN a => WITH 1 as a MATCH (a) RETURN a AS a") {
    val result = projectionInlinedAst("WITH 1 as a MATCH (a) RETURN a")

    result should equal(ast("WITH 1 as a MATCH (a) RETURN a AS `a`"))
  }

  test("should inline multiple identifiers across multiple WITH clauses: WITH 1 as n WITH n+1 AS m RETURN m => RETURN 1+1 as m") {
    val result = projectionInlinedAst("WITH 1 as n WITH n+1 AS m RETURN m")

    result should equal(ast("WITH * WITH * RETURN 1+1 as `m`"))
  }

  test("should inline same identifier across multiple WITH clauses, case #1: WITH 1 as n WITH n+1 AS n RETURN n => RETURN 1+1 as n") {
    val result = projectionInlinedAst("WITH 1 as n WITH n+1 AS n RETURN n")

    result should equal(ast("WITH * WITH * RETURN 1+1 as `n`"))
  }

  test("should inline same identifier across multiple WITH clauses, case #2: WITH 1 as n WITH n+2 AS m WITH n + m as n RETURN n => RETURN 1+1+2 as n") {
    val result = projectionInlinedAst("WITH 1 as n WITH n+2 AS m WITH n + m as n RETURN n")

    result should equal(ast("WITH * WITH * WITH * RETURN 1+(1+2) as `n`"))
  }

  test("MATCH p = (a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, NilPathStep)
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, SingleRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r]->(a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, SingleRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r*]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, MultiRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r*]-(a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, MultiRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  private def parseReturnedExpr(queryText: String) =
    projectionInlinedAst(queryText) match {
      case Query(_, SingleQuery(Seq(_, Return(_, ListedReturnItems(Seq(AliasedReturnItem(expr, Identifier("p")))), _, _, _)))) => expr
    }

  private def projectionInlinedAst(queryText: String) = inlineProjections(ast(queryText))

  private def ast(queryText: String) = {
    val parsed = parser.parse(queryText)
    parsed.typedRewrite[Statement](bottomUp(aliasReturnItems))
  }
}

