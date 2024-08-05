/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.rewriters.QuantifiedPathPatternNodeInsertRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ProjectNamedPathsTest extends CypherFunSuite with AstRewritingTestSupport with TestName {

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(projectNamedPaths)

  private def projectionInlinedQppAst(queryText: String) = ast(queryText).endoRewrite(
    QuantifiedPathPatternNodeInsertRewriter.instance
  ).endoRewrite(nameAllPatternElements(new AnonymousVariableNameGenerator)).endoRewrite(projectNamedPaths)

  private def ast(queryText: String) = {
    val parsed = parse(queryText, OpenCypherExceptionFactory(None))
    val exceptionFactory = OpenCypherExceptionFactory(Some(pos))
    val normalized = parsed.endoRewrite(inSequence(normalizeWithAndReturnClauses(exceptionFactory)))
    val checkResult = normalized.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }

  private def parseReturnedExpr(queryText: String) = {
    val query = projectionInlinedAst(queryText).asInstanceOf[Query]
    query.asInstanceOf[SingleQuery].clauses.last.asInstanceOf[Return].returnItems.items.collectFirst {
      case AliasedReturnItem(expr, Variable("p")) => expr
    }.get
  }

  private def parseReturnedQppExpr(queryText: String) = {
    val query = projectionInlinedQppAst(queryText).asInstanceOf[Query]
    query.asInstanceOf[SingleQuery].clauses.last.asInstanceOf[Return].returnItems.items.collectFirst {
      case AliasedReturnItem(expr, Variable("p")) => expr
    }.get
  }

  test("MATCH p = (a) RETURN p") {
    val returns = parseReturnedExpr("MATCH p = (a) RETURN p")

    val expected = PathExpression(
      NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) CALL {RETURN 1} RETURN p") {
    val returns = parseReturnedExpr(testName)

    val expected = PathExpression(
      NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) CALL {WITH a RETURN 1} RETURN p") {
    val returns = parseReturnedExpr(testName)

    val expected = PathExpression(
      NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) CALL {RETURN 1 AS one UNION RETURN 2 as one} RETURN p") {
    val returns = parseReturnedExpr(testName)

    val expected = PathExpression(
      NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) CALL {WITH a RETURN 1 AS one UNION WITH a RETURN 2 as one} RETURN p") {
    val returns = parseReturnedExpr(testName)

    val expected = PathExpression(
      NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("CALL {MATCH p = (a) RETURN p} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val a = varFor("a")
    val p = varFor("p")
    val CALL = {
      val MATCH = Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(MATCH, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  // We can assume that nameAllPatternParts has already run.
  test("MATCH p = (a)-[r]->(b) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b), q = (b)-[s]->(c) CALL {WITH p, q RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val q = varFor("q")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos),
        RelationshipChain(
          NodePattern(Some(b), None, None, None)(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(c, c)(pos),
              AliasedReturnItem(s, s)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p
              )(pos),
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  b,
                  SingleRelationshipPathStep(s, SemanticDirection.OUTGOING, Some(c), NilPathStep()(pos))(pos)
                )(pos))(pos),
                q
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b)<-[s]-(c) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          RelationshipChain(
            NodePattern(Some(a), None, None, None)(pos),
            RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(b), None, None, None)(pos)
          )(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.INCOMING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(s, s)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(c, c)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(
                PathExpression(
                  NodePathStep(
                    a,
                    SingleRelationshipPathStep(
                      r,
                      SemanticDirection.OUTGOING,
                      Some(b),
                      SingleRelationshipPathStep(s, SemanticDirection.INCOMING, Some(c), NilPathStep()(pos))(pos)
                    )(pos)
                  )(pos)
                )(pos),
                p
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(
              PathExpression(
                NodePathStep(
                  a,
                  SingleRelationshipPathStep(
                    r,
                    SemanticDirection.OUTGOING,
                    Some(b),
                    SingleRelationshipPathStep(s, SemanticDirection.INCOMING, Some(c), NilPathStep()(pos))(pos)
                  )(pos)
                )(pos)
              )(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p, a RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a), (b) CALL {WITH p, b RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos),
        NodePattern(Some(b), None, None, None)(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(b, b)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
              AliasedReturnItem(b, b)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p RETURN 1 AS one UNION WITH p RETURN 2 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None
    )(pos)

    val CALL = {
      val LEFT = {
        val WITH1 =
          With(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(a, a)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        val WITH2 =
          With(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(literalInt(1), one)(pos)
              )
            )(pos),
            None,
            None,
            None
          )(pos)

        SingleQuery(List(WITH1, WITH2, RETURN))(pos)
      }
      val RIGHT = {
        val WITH1 =
          With(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(a, a)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        val WITH2 =
          With(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              includeExisting = false,
              Seq(
                AliasedReturnItem(literalInt(2), one)(pos)
              )
            )(pos),
            None,
            None,
            None
          )(pos)

        SingleQuery(List(WITH1, WITH2, RETURN))(pos)
      }
      ImportingWithSubqueryCall(UnionDistinct(LEFT, RIGHT)(pos), None)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p RETURN p") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p RETURN p")
    val a = varFor("a")
    val p = varFor("p")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  // don't project what is already projected
  test("MATCH p = (a) WITH p, a RETURN p") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p, a RETURN p")
    val a = varFor("a")
    val p = varFor("p")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
            AliasedReturnItem(a, a)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p MATCH q = (b) RETURN p, q") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p MATCH q = (b) WITH p, q RETURN p, q")
    val a = varFor("a")
    val b = varFor("b")
    val p = varFor("p")
    val q = varFor("q")

    val MATCH1 =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val WITH1 =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val MATCH2 =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(b), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val WITH2 =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(p, p)(pos),
            AliasedReturnItem(PathExpression(NodePathStep(b, NilPathStep()(pos))(pos))(pos), q)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(p, p)(pos),
            AliasedReturnItem(q, q)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH1, WITH1, MATCH2, WITH2, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p") {
    val returns = parseReturnedExpr("MATCH p = (a)-[r]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(
        varFor("a"),
        SingleRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
      )(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r]->(a) RETURN p") {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r]-(a) RETURN p")

    val expected = PathExpression(
      NodePathStep(
        varFor("b"),
        SingleRelationshipPathStep(varFor("r"), SemanticDirection.INCOMING, Some(varFor("a")), NilPathStep()(pos))(pos)
      )(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a)-[r*]->(b) RETURN p") {
    val returns = parseReturnedExpr("MATCH p = (a)-[r*]->(b) RETURN p")

    val expected = PathExpression(
      NodePathStep(
        varFor("a"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
      )(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (b)<-[r*]-(a) RETURN p AS p") {
    val returns = parseReturnedExpr("MATCH p = (b)<-[r*]-(a) RETURN p AS p")

    val expected = PathExpression(
      NodePathStep(
        varFor("b"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.INCOMING, Some(varFor("a")), NilPathStep()(pos))(pos)
      )(pos)
    ) _

    returns should equal(expected: PathExpression)
  }

  test("MATCH p = (a) ((n)-[r]->(m)-[q]->(o))+ (b) RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        Variable("a")(pos),
        RepeatPathStep.asRepeatPathStep(
          List(
            Variable("n")(pos),
            Variable("r")(pos),
            Variable("m")(pos),
            Variable("q")(pos)
          ),
          Variable("b")(pos),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = ((n)-[r]->(m)-[q]->(o))+ RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        Variable("  UNNAMED0")(pos),
        RepeatPathStep.asRepeatPathStep(
          List(
            Variable("n")(pos),
            Variable("r")(pos),
            Variable("m")(pos),
            Variable("q")(pos)
          ),
          Variable("  UNNAMED1")(pos),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = (a) ((n)-[r]->(m)-[q]->(o))* ((b)-[r2]-(y))* (k) RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        Variable("a")(pos),
        RepeatPathStep.asRepeatPathStep(
          List(Variable("n")(pos), Variable("r")(pos), Variable("m")(pos), Variable("q")(pos)),
          Variable("  UNNAMED0")(pos),
          MultiRelationshipPathStep(
            rel = v"r2",
            toNode = Some(v"k"),
            next = NilPathStep()(pos),
            direction = BOTH
          )(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order")

    val aId = varFor("a")
    val orderId = varFor("order")
    val rId = varFor("r")
    val pId = varFor("p")
    val bId = varFor("b")

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          RelationshipChain(
            NodePattern(Some(aId), None, None, None)(pos),
            RelationshipPattern(Some(rId), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(bId), None, None, None)(pos)
          )(pos)
        ),
        List(),
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                aId,
                SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
              )(pos))(pos),
              pId
            )(pos),
            AliasedReturnItem(literalInt(42), orderId)(pos)
          )
        )(pos),
        Some(OrderBy(List(AscSortItem(orderId)(pos)))(pos)),
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1 as x")

    val aId = varFor("a")
    val rId = varFor("r")
    val bId = varFor("b")

    val WHERE =
      Where(
        greaterThan(
          function(
            "length",
            PathExpression(NodePathStep(
              aId,
              SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
            )(pos))(pos)
          ),
          literalInt(10)
        )
      )(pos)

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          RelationshipChain(
            NodePattern(Some(aId), None, None, None)(pos),
            RelationshipPattern(Some(rId), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(bId), None, None, None)(pos)
          )(pos)
        ),
        List(),
        Some(WHERE)
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          List(
            AliasedReturnItem(literalInt(1), varFor("x"))(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("Aggregating WITH downstreams") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH length(p) as l, count(*) as x WITH l, x RETURN l + x")
    val a = varFor("a")
    val l = varFor("l")
    val x = varFor("x")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val pathExpression = PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos)
    val WITH1 =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(function("length", pathExpression), l)(pos),
            AliasedReturnItem(CountStar()(pos), x)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val WITH2 =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(l, l)(pos),
            AliasedReturnItem(x, x)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(add(l, x), varFor("l + x"))(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH1, WITH2, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("WHERE and ORDER BY on WITH clauses should be rewritten") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH a ORDER BY p WHERE length(p) = 1 RETURN a")

    val aId = varFor("a")

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(aId), None, None, None)(pos)
        ),
        List(),
        None
      )(pos)

    val pathExpression = PathExpression(NodePathStep(aId, NilPathStep()(pos))(pos))(pos)

    val WHERE =
      Where(
        equals(
          function("length", pathExpression),
          literalInt(1)
        )
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          Seq(
            AliasedReturnItem(aId, aId)(pos)
          )
        )(pos),
        Some(OrderBy(List(AscSortItem(pathExpression)(pos)))(pos)),
        None,
        None,
        Some(WHERE)
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          includeExisting = false,
          List(
            AliasedReturnItem(aId, aId)(pos)
          )
        )(pos),
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("Shortest path with predicate and path assignment, 1 relationship, OUTGOING") {
    val returns = parseReturnedExpr("MATCH p = ANY SHORTEST ((a)-[r]->+(b) WHERE a.prop IS NOT NULL) RETURN p")

    val expectedPathExpression =
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = MultiRelationshipPathStep(
            rel = v"r",
            toNode = Some(v"b"),
            next = NilPathStep()(pos),
            direction = OUTGOING
          )(pos)
        )(pos)
      )(pos)

    returns shouldEqual expectedPathExpression
  }

  test("Shortest path with predicate and path assignment, 1 relationship, INCOMING") {
    val returns = parseReturnedExpr("MATCH p = ANY SHORTEST ((a)<-[r]-+(b) WHERE a.prop IS NOT NULL) RETURN p")

    val expectedPathExpression =
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = MultiRelationshipPathStep(
            rel = v"r",
            toNode = Some(v"b"),
            next = NilPathStep()(pos),
            direction = INCOMING
          )(pos)
        )(pos)
      )(pos)

    returns shouldEqual expectedPathExpression
  }

  test("Shortest path with path assignment, 2 relationships") {
    val returns = parseReturnedExpr("MATCH p = ANY SHORTEST ((a) ((a_in)-[r]->(b_in)-[r2]->(c_in))+ (c)) RETURN p")

    val expectedPathExpression =
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = RepeatPathStep(
            variables = Seq(NodeRelPair(v"a_in", v"r"), NodeRelPair(v"b_in", v"r2")),
            toNode = varFor("c"),
            next = NilPathStep()(pos)
          )(pos)
        )(pos)
      )(pos)

    returns shouldEqual expectedPathExpression
  }
}
