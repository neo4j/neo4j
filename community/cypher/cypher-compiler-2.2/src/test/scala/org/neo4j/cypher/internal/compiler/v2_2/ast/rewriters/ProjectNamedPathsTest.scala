/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.{SemanticState, inSequence}
import org.neo4j.graphdb.Direction

class ProjectNamedPathsTest extends CypherFunSuite with AstRewritingTestSupport {

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(projectNamedPaths)

  private def ast(queryText: String) = {
    val parsed = parser.parse(queryText)
    val normalized = parsed.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }

  private def parseReturnedExpr(queryText: String) =
    projectionInlinedAst(queryText) match {
      case Query(_, SingleQuery(Seq(_, Return(_, ReturnItems(_, Seq(AliasedReturnItem(expr, Identifier("p")))), _, _, _)))) => expr
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

  test("MATCH p = (b)<-[r*]-(a) RETURN p AS p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p AS p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, MultiRelationshipPathStep(Identifier("r")_, Direction.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order")

    val aId = Identifier("a")(pos)
    val fresh30: Identifier = Identifier("  FRESHID30")(pos)
    val fresh33: Identifier = Identifier("  FRESHID33")(pos)
    val orderId: Identifier = Identifier("order")(pos)
    val rId = Identifier("r")(pos)
    val pId = Identifier("p")(pos)
    val bId = Identifier("b")(pos)

    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            RelationshipChain(
              NodePattern(Some(aId), List(), None, naked = false)(pos),
              RelationshipPattern(Some(rId), optional = false, List(), None, None, Direction.OUTGOING)(pos), NodePattern(Some(bId), List(), None, naked = false)(pos)
            )(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(aId, SingleRelationshipPathStep(rId, Direction.OUTGOING, NilPathStep)))(pos), fresh30)(pos),
          AliasedReturnItem(SignedDecimalIntegerLiteral("42")(pos), fresh33)(pos)
        ))(pos),
        Some(OrderBy(List(AscSortItem(fresh33)(pos)))(pos)),
        None, None, None
      )(pos)


    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, List(
          AliasedReturnItem(fresh30, pId)(pos),
          AliasedReturnItem(fresh33, orderId)(pos)
        ))(pos),
        None, None, None
      )(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1 as x")

    val aId = Identifier("a")(pos)
    val rId = Identifier("r")(pos)
    val bId = Identifier("b")(pos)

    val WHERE =
      Where(
        GreaterThan(
          FunctionInvocation(FunctionName("length")(pos), PathExpression(NodePathStep(aId, SingleRelationshipPathStep(rId, Direction.OUTGOING, NilPathStep)))(pos))(pos),
          SignedDecimalIntegerLiteral("10")(pos)
        )(pos)
      )(pos)

    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            RelationshipChain(
              NodePattern(Some(aId), List(), None, naked = false)(pos),
              RelationshipPattern(Some(rId), optional = false, List(), None, None, Direction.OUTGOING)(pos), NodePattern(Some(bId), List(), None, naked = false)(pos)
            )(pos))
        ))(pos), List(), Some(WHERE))(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, List(
          AliasedReturnItem(SignedDecimalIntegerLiteral("1")(pos), Identifier("x")(pos))(pos)
        ))(pos),
        None, None, None
      )(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }
}
