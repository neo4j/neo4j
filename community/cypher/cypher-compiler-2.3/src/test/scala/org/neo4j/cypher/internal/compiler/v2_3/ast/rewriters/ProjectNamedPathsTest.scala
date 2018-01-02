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

import org.neo4j.cypher.internal.compiler.v2_3.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v2_3.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SemanticState, inSequence}

class ProjectNamedPathsTest extends CypherFunSuite with AstRewritingTestSupport {

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(projectNamedPaths)

  private def ast(queryText: String) = {
    val parsed = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val normalized = parsed.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }

  private def parseReturnedExpr(queryText: String) =
    projectionInlinedAst(queryText) match {
      case Query(_, SingleQuery(Seq(_, Return(_, ReturnItems(_, Seq(AliasedReturnItem(expr, Identifier("p")))), _, _, _, _)))) => expr
    }

  test("MATCH p = (a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, NilPathStep)
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) WITH p RETURN p" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p RETURN p")
    val a = Identifier("a")(pos)
    val p = Identifier("p")(pos)
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None, naked = false)(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos)
        ))(pos), None, None, None, None)(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos)
        ))(pos), None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  //don't project what is already projected
  test("MATCH p = (a) WITH p, a RETURN p" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p, a RETURN p")
    val a = Identifier("a")(pos)
    val p = Identifier("p")(pos)
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None, naked = false)(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos),
          AliasedReturnItem(a, a)(pos)
        ))(pos), None, None, None, None)(pos)

    val RETURN=
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos)
        ))(pos), None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p MATCH q = (b) RETURN p, q" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p MATCH q = (b) WITH p, q RETURN p, q")
    val a = Identifier("a")(pos)
    val b = Identifier("b")(pos)
    val p = Identifier("p")(pos)
    val q = Identifier("q")(pos)

    val MATCH1 =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None, naked = false)(pos))
        ))(pos), List(), None)(pos)

    val WITH1 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos)
        ))(pos), None, None, None, None)(pos)

    val MATCH2 =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(b), List(), None, naked = false)(pos))
        ))(pos), List(), None)(pos)

    val WITH2 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos),
          AliasedReturnItem(PathExpression(NodePathStep(b, NilPathStep))(pos), q)(pos)
        ))(pos), None, None, None, None)(pos)

    val RETURN=
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos),
          AliasedReturnItem(q, q)(pos)
        ))(pos), None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH1, WITH1, MATCH2, WITH2, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r]->(a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r*]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Identifier("a")_, MultiRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r*]-(a) RETURN p AS p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p AS p")

    val expected = PathExpression(
      NodePathStep(Identifier("b")_, MultiRelationshipPathStep(Identifier("r")_, SemanticDirection.INCOMING, NilPathStep))
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
              RelationshipPattern(Some(rId), optional = false, List(), None, None, SemanticDirection.OUTGOING)(pos), NodePattern(Some(bId), List(), None, naked = false)(pos)
            )(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(aId, SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, NilPathStep)))(pos), fresh30)(pos),
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
          FunctionInvocation(FunctionName("length")(pos), PathExpression(NodePathStep(aId, SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, NilPathStep)))(pos))(pos),
          SignedDecimalIntegerLiteral("10")(pos)
        )(pos)
      )(pos)

    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            RelationshipChain(
              NodePattern(Some(aId), List(), None, naked = false)(pos),
              RelationshipPattern(Some(rId), optional = false, List(), None, None, SemanticDirection.OUTGOING)(pos), NodePattern(Some(bId), List(), None, naked = false)(pos)
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

  test("Aggregating WITH downstreams" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH length(p) as l, count(*) as x WITH l, x RETURN l + x")
    val a = ident("a")
    val p = ident("p")
    val l = ident("l")
    val x = ident("x")
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None, naked = false)(pos))
        ))(pos), List(), None)(pos)

    val pathExpression = PathExpression(NodePathStep(a, NilPathStep))(pos)
    val WITH1 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(FunctionInvocation(FunctionName("length")(pos), pathExpression)(pos), l)(pos),
          AliasedReturnItem(CountStar()(pos), x)(pos)
        ))(pos), None, None, None, None)(pos)

    val WITH2 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(l, l)(pos),
          AliasedReturnItem(x, x)(pos)
        ))(pos), None, None, None, None)(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(Add(l, x)(pos), ident("l + x"))(pos)
        ))(pos), None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH1, WITH2, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }
}
