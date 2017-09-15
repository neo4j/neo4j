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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.apa.v3_4.inSequence
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_4.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{expandStar, normalizeReturnClauses, normalizeWithClauses, projectNamedPaths}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticDirection, SemanticState}

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
      case Query(_, SingleQuery(Seq(_, Return(_, ReturnItems(_, Seq(AliasedReturnItem(expr, Variable("p")))), _, _, _, _, _)))) => expr
    }

  test("MATCH p = (a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Variable("a")_, NilPathStep)
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) WITH p RETURN p" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p RETURN p")
    val a = Variable("a")(pos)
    val p = Variable("p")(pos)
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None)(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos)
        ))(pos), None, None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  //don't project what is already projected
  test("MATCH p = (a) WITH p, a RETURN p" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p, a RETURN p")
    val a = Variable("a")(pos)
    val p = Variable("p")(pos)
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None)(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos),
          AliasedReturnItem(a, a)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val RETURN=
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos)
        ))(pos), None, None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p MATCH q = (b) RETURN p, q" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p MATCH q = (b) WITH p, q RETURN p, q")
    val a = Variable("a")(pos)
    val b = Variable("b")(pos)
    val p = Variable("p")(pos)
    val q = Variable("q")(pos)

    val MATCH1 =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None)(pos))
        ))(pos), List(), None)(pos)

    val WITH1 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep))(pos), p)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val MATCH2 =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(b), List(), None)(pos))
        ))(pos), List(), None)(pos)

    val WITH2 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos),
          AliasedReturnItem(PathExpression(NodePathStep(b, NilPathStep))(pos), q)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val RETURN=
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(p, p)(pos),
          AliasedReturnItem(q, q)(pos)
        ))(pos), None, None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH1, WITH1, MATCH2, WITH2, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Variable("a")_, SingleRelationshipPathStep(Variable("r")_, SemanticDirection.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r]->(a) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(Variable("b")_, SingleRelationshipPathStep(Variable("r")_, SemanticDirection.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r*]->(b) RETURN p" ) {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(Variable("a")_, MultiRelationshipPathStep(Variable("r")_, SemanticDirection.OUTGOING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r*]-(a) RETURN p AS p" ) {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p AS p")

    val expected = PathExpression(
      NodePathStep(Variable("b")_, MultiRelationshipPathStep(Variable("r")_, SemanticDirection.INCOMING, NilPathStep))
    )_

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order")

    val aId = Variable("a")(pos)
    val fresh30: Variable = Variable("  FRESHID30")(pos)
    val fresh33: Variable = Variable("  FRESHID33")(pos)
    val orderId: Variable = Variable("order")(pos)
    val rId = Variable("r")(pos)
    val pId = Variable("p")(pos)
    val bId = Variable("b")(pos)

    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            RelationshipChain(
              NodePattern(Some(aId), List(), None)(pos),
              RelationshipPattern(Some(rId), List(), None, None, SemanticDirection.OUTGOING)(pos), NodePattern(Some(bId), List(), None)(pos)
            )(pos))
        ))(pos), List(), None)(pos)

    val WITH =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(PathExpression(NodePathStep(aId, SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, NilPathStep)))(pos), fresh30)(pos),
          AliasedReturnItem(SignedDecimalIntegerLiteral("42")(pos), fresh33)(pos)
        ))(pos), PassAllGraphReturnItems(pos),
        Some(OrderBy(List(AscSortItem(fresh33)(pos)))(pos)),
        None, None, None
      )(pos)


    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, List(
          AliasedReturnItem(fresh30, pId)(pos),
          AliasedReturnItem(fresh33, orderId)(pos)
        ))(pos),
        None, None, None, None
      )(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1 as x")

    val aId = Variable("a")(pos)
    val rId = Variable("r")(pos)
    val bId = Variable("b")(pos)

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
              NodePattern(Some(aId), List(), None)(pos),
              RelationshipPattern(Some(rId), List(), None, None, SemanticDirection.OUTGOING)(pos), NodePattern(Some(bId), List(), None)(pos)
            )(pos))
        ))(pos), List(), Some(WHERE))(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, List(
          AliasedReturnItem(SignedDecimalIntegerLiteral("1")(pos), Variable("x")(pos))(pos)
        ))(pos),
        None, None, None, None
      )(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }

  test("Aggregating WITH downstreams" ) {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH length(p) as l, count(*) as x WITH l, x RETURN l + x")
    val a = varFor("a")
    val p = varFor("p")
    val l = varFor("l")
    val x = varFor("x")
    val MATCH =
      Match(optional = false,
        Pattern(List(
          EveryPath(
            NodePattern(Some(a), List(), None)(pos))
        ))(pos), List(), None)(pos)

    val pathExpression = PathExpression(NodePathStep(a, NilPathStep))(pos)
    val WITH1 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(FunctionInvocation(FunctionName("length")(pos), pathExpression)(pos), l)(pos),
          AliasedReturnItem(CountStar()(pos), x)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val WITH2 =
      With(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(l, l)(pos),
          AliasedReturnItem(x, x)(pos)
        ))(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

    val RETURN =
      Return(distinct = false,
        ReturnItems(includeExisting = false, Seq(
          AliasedReturnItem(Add(l, x)(pos), varFor("l + x"))(pos)
        ))(pos), None, None, None, None)(pos)

    val expected: Query = Query(None, SingleQuery(List(MATCH, WITH1, WITH2, RETURN))(pos))(pos)

    rewritten should equal(expected)
  }
}
