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
import org.neo4j.cypher.internal.compiler.v2_1.ast.PathExpression
import org.neo4j.cypher.internal.compiler.v2_1.ast.ListedReturnItems
import org.neo4j.cypher.internal.compiler.v2_1.ast.MultiRelationshipPathStep
import org.neo4j.cypher.internal.compiler.v2_1.ast.NodePathStep
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.Return
import org.neo4j.cypher.internal.compiler.v2_1.ast.SingleQuery
import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.neo4j.cypher.internal.compiler.v2_1.ast.SingleRelationshipPathStep
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp

class InlineProjectionsTest extends CypherFunSuite with AstRewritingTestSupport {
  // TODO: multiple withes + shadowing
  // TODO: local scope (extract, ...)

  test("should inline: MATCH n WITH n AS m RETURN m => MATCH n RETURN n") {
    val result = projectionInlinedAst("MATCH n WITH n AS m RETURN m")

    result should equal(ast("MATCH n WITH * RETURN n AS m"))
  }

  test("should inline: WITH 1 AS x RETURN 1 + x => WITH * RETURN 1 + 1") {
    val result = projectionInlinedAst("WITH 1 AS x RETURN 1 + x")

    result should equal(ast("WITH * RETURN 1 + 1 AS `1 + x`"))
  }

  test("match p = (a) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, NilPathStep)
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (a)-[r]->(b) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, SingleRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (b)<-[r]->(a) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, SingleRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (a)-[r*]->(b) return p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, MultiRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("match p = (b)<-[r*]-(a) return p" ) {
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

